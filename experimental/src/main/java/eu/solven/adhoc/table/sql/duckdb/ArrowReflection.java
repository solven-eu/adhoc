package eu.solven.adhoc.table.sql.duckdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

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
	 * For example, {@code VarCharVector.getObject()} returns an {@code org.apache.arrow.vector.util.Text} instance
	 * rather than a {@link String}; this method normalises such values.
	 */
	static Object convertValue(Object value) {
		if (value == null) {
			return null;
		}
		// Arrow VarCharVector returns org.apache.arrow.vector.util.Text
		if ("org.apache.arrow.vector.util.Text".equals(value.getClass().getName())) {
			return value.toString();
		}
		if (value instanceof BigDecimal bigD && bigD.scale() <= 0) {
			// Converts int/long to BigInteger
			return bigD.toBigInteger();
		}
		return value;
	}
}