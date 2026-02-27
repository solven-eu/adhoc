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
import java.util.List;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordFactory;
import eu.solven.adhoc.data.row.TabularRecordBuilder;
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

	// Time-unit conversion constants used for Arrow temporal vectors.
	// See https://arrow.apache.org/docs/format/Columnar.html#date-layout
	// See https://arrow.apache.org/docs/format/Columnar.html#timestamp-layout
	private static final long MILLIS_PER_DAY = 86_400_000L;
	private static final long MICROS_PER_SECOND = 1_000_000L;
	private static final long NANOS_PER_MICRO = 1_000L;
	private static final long NANOS_PER_SECOND = 1_000_000_000L;

	static final Method LOAD_NEXT_BATCH;
	static final Method GET_VECTOR_SCHEMA_ROOT;
	static final Method GET_ROW_COUNT;
	static final Method GET_FIELD_VECTORS;
	static final Method GET_OBJECT;

	// VectorSchemaRoot.slice(int index, int length) creates a new root sharing the same ArrowBuf
	// objects via reference counting. Safe to use after loadNextBatch() because the original
	// vectors decrement the ref-count (but don't free) when they are cleared for the next batch.
	// See https://arrow.apache.org/docs/java/vector_schema_root.html
	static final Method SLICE;

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
			SLICE = rootClass.getMethod("slice", int.class, int.class);

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
	 * Closes a {@code VectorSchemaRoot}, decrementing the reference counts of its underlying {@code ArrowBuf} objects.
	 * Must be called on every sliced root produced by {@link #SLICE} once the slice is fully consumed.
	 */
	static void closeRoot(Object root) {
		try {
			root.getClass().getMethod("close").invoke(root);
		} catch (Exception e) {
			log.warn("Error closing Arrow VectorSchemaRoot", e);
		}
	}

	/**
	 * Reads one value from a vector, applying type conversion via {@link #convertValue(Object, Object)}.
	 */
	static Object getVectorValue(List<?> vectors, int vectorIndex, int rowIndex) {
		Object vector = vectors.get(vectorIndex);
		try {
			Object raw = GET_OBJECT.invoke(vector, rowIndex);
			return convertValue(raw, vector);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error reading Arrow vector value", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	/**
	 * Builds a {@link ITabularRecord} for {@code rowIndex} from the given vectors.
	 */
	static ITabularRecord buildRecord(List<?> vectors, int rowIndex, ITabularRecordFactory factory) {
		TabularRecordBuilder builder = factory.makeTabularRecordBuilder();

		List<String> aggregates = factory.getAggregates();
		List<String> columns = factory.getColumns();

		int vectorIndex = 0;

		for (int i = 0; i < aggregates.size(); i++, vectorIndex++) {
			Object value = getVectorValue(vectors, vectorIndex, rowIndex);
			if (value != null) {
				builder.appendAggregate(aggregates.get(i), value);
			}
		}

		for (int i = 0; i < columns.size(); i++, vectorIndex++) {
			builder.appendGroupBy(getVectorValue(vectors, vectorIndex, rowIndex));
		}

		return builder.build();
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
				long len = (long) TEXT_GET_LENGTH.invoke(value);
				return Utf8ByteSlice.builder().byteSlice(IByteSlice.wrap(buf, Ints.checkedCast(len))).build();
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
		case "DateMilliVector" -> LocalDate.ofEpochDay((Long) value / MILLIS_PER_DAY);
		case "TimeStampSecVector", "TimeStampSecTZVector" -> Instant.ofEpochSecond((Long) value);
		case "TimeStampMilliVector", "TimeStampMilliTZVector" -> Instant.ofEpochMilli((Long) value);
		case "TimeStampMicroVector", "TimeStampMicroTZVector" -> {
			long micros = (Long) value;
			yield Instant.ofEpochSecond(micros / MICROS_PER_SECOND, micros % MICROS_PER_SECOND * NANOS_PER_MICRO);
		}
		case "TimeStampNanoVector", "TimeStampNanoTZVector" -> {
			long nanos = (Long) value;
			yield Instant.ofEpochSecond(nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND);
		}
		default -> convertValue(value);
		};
	}
}