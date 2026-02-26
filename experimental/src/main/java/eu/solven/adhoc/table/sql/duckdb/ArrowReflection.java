/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.table.sql.duckdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import eu.solven.adhoc.encoding.bytes.Utf8ByteSlice;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds Arrow API methods resolved once via reflection. Arrow is an optional runtime dependency: the class is only
 * initialised when first referenced (i.e. on the first Arrow-based query).
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
final class ArrowReflection {

	static final Method LOAD_NEXT_BATCH;
	static final Method GET_VECTOR_SCHEMA_ROOT;
	static final Method GET_ROW_COUNT;
	static final Method GET_FIELD_VECTORS;
	static final Method GET_OBJECT;

	// Arrow's Text (org.apache.arrow.vector.util.Text) wraps a byte[] with a valid-length field.
	// We reflect these to extract the raw bytes without decoding to String.
	// See https://github.com/apache/arrow/blob/main/java/vector/src/main/java/org/apache/arrow/vector/util/Text.java
	static final Method TEXT_GET_BYTES;
	static final Method TEXT_GET_LENGTH;

	static {
		try {
			// module: arrow-vector
			Class<?> readerClass = Class.forName("org.apache.arrow.vector.ipc.ArrowReader");
			LOAD_NEXT_BATCH = readerClass.getMethod("loadNextBatch");
			GET_VECTOR_SCHEMA_ROOT = readerClass.getMethod("getVectorSchemaRoot");

			Class<?> rootClass = Class.forName("org.apache.arrow.vector.VectorSchemaRoot");
			GET_ROW_COUNT = rootClass.getMethod("getRowCount");
			GET_FIELD_VECTORS = rootClass.getMethod("getFieldVectors");

			Class<?> vectorClass = Class.forName("org.apache.arrow.vector.ValueVector");
			GET_OBJECT = vectorClass.getMethod("getObject", int.class);

			Class<?> textClass = Class.forName("org.apache.arrow.vector.util.Text");
			TEXT_GET_BYTES = textClass.getMethod("getBytes");
			TEXT_GET_LENGTH = textClass.getMethod("getLength");
		} catch (ClassNotFoundException | NoSuchMethodException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	static Object createAllocator() {
		// module: arrow-memory-core
		try {
			Class<?> allocatorClass = Class.forName("org.apache.arrow.memory.RootAllocator");
			return allocatorClass.getDeclaredConstructor(long.class).newInstance(Long.MAX_VALUE);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new IllegalStateException("Failed to create Arrow RootAllocator", e);
		}
	}

	static void closeAllocator(Object allocator) {
		try {
			allocator.getClass().getMethod("close").invoke(allocator);
		} catch (Exception e) {
			log.warn("Error closing Arrow allocator", e);
		}
	}

	static void closeReader(Object reader) {
		try {
			reader.getClass().getMethod("close").invoke(reader);
		} catch (Exception e) {
			log.warn("Error closing Arrow reader", e);
		}
	}

	/**
	 * Converts Arrow-specific value types to plain Java types expected by the rest of the pipeline.
	 *
	 * <p>
	 * {@code VarCharVector.getObject()} returns an {@code org.apache.arrow.vector.util.Text} wrapping a UTF-8
	 * byte-buffer. To avoid a UTF-8&nbsp;→&nbsp;UTF-16&nbsp;→&nbsp;UTF-8 round-trip when the value is later
	 * FSST-compressed, this method wraps the raw buffer in an {@link AdhocUtf8} (zero copy) instead of decoding to a
	 * {@link String}.
	 */
	static Object convertValue(Object value) {
		if (value == null) {
			return null;
		}
		// Arrow VarCharVector returns org.apache.arrow.vector.util.Text
		if ("org.apache.arrow.vector.util.Text".equals(value.getClass().getName())) {
			try {
				byte[] buf = (byte[]) TEXT_GET_BYTES.invoke(value);
				int len = (int) TEXT_GET_LENGTH.invoke(value);
				return Utf8ByteSlice.builder().byteSlice(IByteSlice.wrap(buf, len)).build();
			} catch (InvocationTargetException | IllegalAccessException e) {
				throw new IllegalStateException("Failed to extract bytes from Arrow Text", e);
			}
		}
		if (value instanceof BigDecimal bigD && bigD.scale() <= 0) {
			// Converts int/long to BigInteger
			return bigD.toBigInteger();
		}
		return value;
	}

	/**
	 * Converts an Arrow-specific value to a plain Java type, using the vector's runtime class to resolve temporal types
	 * that Arrow encodes as raw numbers (e.g. {@code DateDayVector} returns days-since-epoch as {@link Integer}).
	 *
	 * <p>
	 * See https://arrow.apache.org/docs/java/vector.html for the mapping between Arrow vector types and Java values.
	 */
	// TODO Should prepare converter given the schema once instead of per value
	static Object convertValue(Object value, Object vector) {
		if (value == null) {
			return null;
		}
		// See https://arrow.apache.org/docs/format/Columnar.html#date-layout
		// See https://arrow.apache.org/docs/format/Columnar.html#timestamp-layout
		return switch (vector.getClass().getSimpleName()) {
		case "DateDayVector" -> LocalDate.ofEpochDay((Integer) value);
		case "DateMilliVector" -> LocalDate.ofEpochDay((Long) value / 86_400_000L);
		case "TimeStampSecVector", "TimeStampSecTZVector" -> Instant.ofEpochSecond((Long) value);
		case "TimeStampMilliVector", "TimeStampMilliTZVector" -> Instant.ofEpochMilli((Long) value);
		case "TimeStampMicroVector", "TimeStampMicroTZVector" -> {
			long micros = (Long) value;
			yield Instant.ofEpochSecond(micros / 1_000_000L, micros % 1_000_000L * 1_000L);
		}
		case "TimeStampNanoVector", "TimeStampNanoTZVector" -> {
			long nanos = (Long) value;
			yield Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L);
		}
		default -> convertValue(value);
		};
	}
}