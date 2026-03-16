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
package eu.solven.adhoc.table.arrow;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.dataframe.row.TabularRecordBuilder;
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
public final class ArrowReflection {

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

	static final Method GET_FIELD;
	static final Method FIELD_GET_NAME;

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
	static final Constructor<?> ARROW_STREAM_READER_CTOR;

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
			GET_FIELD = vectorClass.getMethod("getField");

			Class<?> fieldClass = Class.forName("org.apache.arrow.vector.types.pojo.Field");
			FIELD_GET_NAME = fieldClass.getMethod("getName");

			Class<?> textClass = Class.forName("org.apache.arrow.vector.util.Text");
			TEXT_GET_BYTES = textClass.getMethod("getBytes");
			TEXT_GET_LENGTH = textClass.getMethod("getLength");

			Class<?> allocatorClass = Class.forName("org.apache.arrow.memory.BufferAllocator");
			Class<?> streamReaderClass = Class.forName("org.apache.arrow.vector.ipc.ArrowStreamReader");
			ARROW_STREAM_READER_CTOR = streamReaderClass.getConstructor(InputStream.class, allocatorClass);
		} catch (ClassNotFoundException | NoSuchMethodException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public static Object createAllocator() {
		// module: arrow-memory-core
		try {
			Class<?> allocatorClass = Class.forName("org.apache.arrow.memory.RootAllocator");
			return allocatorClass.getDeclaredConstructor(long.class).newInstance(Long.MAX_VALUE);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new IllegalStateException("Failed to create Arrow RootAllocator", e);
		}
	}

	public static Object createStreamReader(InputStream inputStream, Object allocator) {
		try {
			return ARROW_STREAM_READER_CTOR.newInstance(inputStream, allocator);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new IllegalStateException("Failed to create Arrow stream reader", e);
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
	 *
	 * <p>
	 * Grouping-set absent columns are detected by looking for indicator vectors named {@code grouping_<col>_} (same
	 * convention as {@code JooqTableQueryFactory.groupingAlias}): a value of {@code 0} means the column is present in
	 * this grouping set; any other value means it is absent.
	 *
	 * @see eu.solven.adhoc.table.sql.JooqTableWrapper.intoTabularRecord(ITabularRecordFactory, Record)
	 */
	static ITabularRecord buildRecord(List<?> vectors, int rowIndex, ITabularRecordFactory factory) {
		// Build a name→index map to allow fast lookup of grouping indicator vectors.
		Map<String, Integer> nameToIndex = new LinkedHashMap<>();
		for (int i = 0; i < vectors.size(); i++) {
			nameToIndex.put(getVectorName(vectors.get(i)), i);
		}

		// Detect absent optional columns using grouping indicator vectors.
		// A non-zero grouping indicator means the column is absent from this grouping set.
		Set<String> absentColumns = factory.getOptionalColumns().stream().filter(c -> {
			String indicatorName = groupingAlias(c);
			Integer indicatorIdx = nameToIndex.get(indicatorName);
			if (indicatorIdx == null) {
				return false;
			}
			Object indicatorValue = getVectorValue(vectors, indicatorIdx, rowIndex);
			return !Long.valueOf(0).equals(indicatorValue);
		}).collect(ImmutableSet.toImmutableSet());

		TabularRecordBuilder builder = factory.makeTabularRecordBuilder(absentColumns);

		List<String> aggregates = factory.getAggregates();
		ImmutableList<String> columns = factory.getColumns().asList();

		int vectorIndex = 0;

		for (int i = 0; i < aggregates.size(); i++, vectorIndex++) {
			Object value = getVectorValue(vectors, vectorIndex, rowIndex);
			if (value != null) {
				builder.appendAggregate(aggregates.get(i), value);
			}
		}

		int nbToAppend = columns.size() - absentColumns.size();
		int nbAppended = 0;
		for (int i = 0; i < columns.size() && nbAppended < nbToAppend; i++, vectorIndex++) {
			String col = columns.get(i);
			if (absentColumns.contains(col)) {
				log.debug("Skip NULL as {} not in current GROUPING SET", col);
				continue;
			}
			builder.appendGroupBy(getVectorValue(vectors, vectorIndex, rowIndex));
			nbAppended++;
		}

		return builder.build();
	}

	/**
	 * Returns the grouping indicator vector name for a column, matching the convention in
	 * {@code JooqTableQueryFactory.groupingAlias}.
	 */
	private static String groupingAlias(String column) {
		return "grouping_" + column.replaceAll("[\".]", "") + "_";
	}

	/**
	 * Converts Arrow-specific value types to plain Java types expected by the rest of the pipeline.
	 *
	 * <p>
	 * {@code VarCharVector.getObject()} returns an {@code org.apache.arrow.vector.util.Text} wrapping a UTF-8
	 * byte-buffer. To avoid a UTF-8&nbsp;→&nbsp;UTF-16&nbsp;→&nbsp;UTF-8 round-trip when the value is later
	 * FSST-compressed, this method wraps the raw buffer in an {@link Utf8ByteSlice} (zero copy) instead of decoding to
	 * a {@link String}.
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

	public static List<String> getFieldNames(Object currentRoot) {
		try {
			List<?> vectors = (List<?>) GET_FIELD_VECTORS.invoke(currentRoot);
			List<String> names = new ArrayList<>(vectors.size());
			for (Object vector : vectors) {
				names.add(getVectorName(vector));
			}
			return ImmutableList.copyOf(names);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error getting field names from Arrow VectorSchemaRoot", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	private static String getVectorName(Object vector) {
		try {
			Object field = GET_FIELD.invoke(vector);
			return (String) FIELD_GET_NAME.invoke(field);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error getting vector field name", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}
}
