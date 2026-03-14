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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.tabular.ColumnarTabularView;
import eu.solven.adhoc.data.tabular.IReadableTabularView;
import eu.solven.adhoc.data.tabular.ITabularViewArrowSerializer;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts an {@link IReadableTabularView} into an Apache Arrow IPC stream written to a {@link WritableByteChannel}.
 * The result uses {@code application/vnd.apache.arrow.stream} encoding and can be decoded by any Arrow-compatible
 * client (e.g. {@code apache-arrow} JS library, PyArrow, DuckDB).
 *
 * <p>
 * When the input is a {@link ColumnarTabularView} the column lists are used directly, avoiding the per-row map
 * transposition that would be required for a row-oriented view. For all other view types the data is loaded into a
 * {@link ColumnarTabularView} first.
 *
 * <p>
 * Coordinate columns appear first in the Arrow schema, followed by measure (aggregate) columns. Column types are
 * inferred from the first non-null value; mixed-type columns fall back to {@code Utf8}.
 *
 * <p>
 * Supported Java → Arrow type mappings:
 * <ul>
 * <li>{@link Long} → Int64</li>
 * <li>{@link Integer} → Int32</li>
 * <li>{@link Double} / {@link Float} → Float64</li>
 * <li>{@link Boolean} → Bool</li>
 * <li>{@link LocalDate} → Date32 (days since epoch)</li>
 * <li>Everything else → Utf8</li>
 * </ul>
 *
 * <p>
 * Registered as a {@link java.util.ServiceLoader} provider so that modules without a compile-time Arrow dependency can
 * discover this implementation at runtime.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TabularViewArrowSerializer implements ITabularViewArrowSerializer {

	@Override
	public void serialize(IReadableTabularView view, WritableByteChannel channel) throws IOException {
		ColumnarTabularView columnar = ColumnarTabularView.load(view);

		Map<String, List<?>> coordCols = columnar.getCoordinateColumns();
		Map<String, List<?>> aggCols = columnar.getAggregateColumns();

		Map<String, ArrowType> coordTypes = inferTypes(coordCols);
		Map<String, ArrowType> aggTypes = inferTypes(aggCols);

		List<Field> fields = buildFields(coordCols.keySet(), coordTypes, aggCols.keySet(), aggTypes);
		Schema schema = new Schema(fields);

		int rowCount = Ints.checkedCast(columnar.size());

		try (BufferAllocator allocator = new RootAllocator();
				VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
				ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {

			root.allocateNew();
			populateVectors(root, coordCols, aggCols, rowCount);
			root.setRowCount(rowCount);

			writer.start();
			writer.writeBatch();
			writer.end();
		}
	}

	protected List<Field> buildFields(Set<String> coordKeys,
			Map<String, ArrowType> coordTypes,
			Set<String> aggKeys,
			Map<String, ArrowType> aggTypes) {
		List<Field> fields = new ArrayList<>(coordKeys.size() + aggKeys.size());
		for (String col : coordKeys) {
			fields.add(Field.nullable(col, coordTypes.get(col)));
		}
		for (String col : aggKeys) {
			fields.add(Field.nullable(col, aggTypes.get(col)));
		}
		return fields;
	}

	protected void populateVectors(VectorSchemaRoot root,
			Map<String, List<?>> coordCols,
			Map<String, List<?>> aggCols,
			int rowCount) {
		int fieldIndex = 0;
		for (Map.Entry<String, List<?>> entry : coordCols.entrySet()) {
			fillVector(root.getVector(fieldIndex++), entry.getValue(), rowCount);
		}
		for (Map.Entry<String, List<?>> entry : aggCols.entrySet()) {
			fillVector(root.getVector(fieldIndex++), entry.getValue(), rowCount);
		}
	}

	protected void fillVector(FieldVector vector, List<?> columnValues, int rowCount) {
		for (int row = 0; row < rowCount; row++) {
			setVectorValue(vector, row, columnValues.get(row));
		}
	}

	protected Map<String, ArrowType> inferTypes(Map<String, List<?>> columns) {
		Map<String, ArrowType> types = new LinkedHashMap<>();
		for (Map.Entry<String, List<?>> entry : columns.entrySet()) {
			types.put(entry.getKey(), inferColumnType(entry.getValue()));
		}
		return types;
	}

	protected ArrowType inferColumnType(List<?> values) {
		Class<?> inferredClass = null;
		for (Object val : values) {
			if (val == null) {
				continue;
			}
			if (inferredClass == null) {
				inferredClass = val.getClass();
			} else if (!inferredClass.equals(val.getClass())) {
				return ArrowType.Utf8.INSTANCE;
			}
		}
		return arrowTypeFor(inferredClass);
	}

	protected ArrowType arrowTypeFor(Class<?> javaType) {
		if (javaType == null) {
			return ArrowType.Utf8.INSTANCE;
		}
		if (javaType == Long.class) {
			return new ArrowType.Int(64, true);
		}
		if (javaType == Integer.class) {
			return new ArrowType.Int(32, true);
		}
		if (javaType == Double.class || javaType == Float.class) {
			return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
		}
		if (javaType == Boolean.class) {
			return ArrowType.Bool.INSTANCE;
		}
		if (javaType == LocalDate.class) {
			return new ArrowType.Date(DateUnit.DAY);
		}
		return ArrowType.Utf8.INSTANCE;
	}

	protected void setVectorValue(FieldVector vector, int index, Object value) {
		if (value == null) {
			// Arrow vectors are null by default after allocateNew(); no action needed
			return;
		}
		if (vector instanceof BigIntVector v) {
			v.setSafe(index, ((Number) value).longValue());
		} else if (vector instanceof IntVector v) {
			v.setSafe(index, ((Number) value).intValue());
		} else if (vector instanceof Float8Vector v) {
			v.setSafe(index, ((Number) value).doubleValue());
		} else if (vector instanceof BitVector v) {
			v.setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
		} else if (vector instanceof DateDayVector v) {
			v.setSafe(index, Ints.checkedCast(((LocalDate) value).toEpochDay()));
		} else if (vector instanceof VarCharVector v) {
			v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
		} else {
			log.warn("Unhandled Arrow vector type {} for value type {}",
					vector.getClass().getSimpleName(),
					value.getClass().getSimpleName());
		}
	}
}
