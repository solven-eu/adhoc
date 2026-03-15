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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.dataframe.tabular.ColumnarTabularView;
import eu.solven.adhoc.dataframe.tabular.ListBasedTabularView;

/**
 * Unit tests for {@link TabularViewArrowSerializer}. Each test serializes a view and then deserializes the resulting
 * Arrow IPC stream to verify correctness.
 *
 * @author Benoit Lacelle
 */
public class TestTabularViewArrowSerializer {

	final TabularViewArrowSerializer serializer = new TabularViewArrowSerializer();

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	protected byte[] roundTrip(ListBasedTabularView view) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serializer.serialize(view, Channels.newChannel(out));
		return out.toByteArray();
	}

	protected byte[] roundTripColumnar(ColumnarTabularView view) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serializer.serialize(view, Channels.newChannel(out));
		return out.toByteArray();
	}

	protected VectorSchemaRoot readBatch(byte[] bytes, BufferAllocator allocator) throws IOException {
		ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator);
		VectorSchemaRoot root = reader.getVectorSchemaRoot();
		reader.loadNextBatch();
		return root;
	}

	// -------------------------------------------------------------------------
	// Empty view
	// -------------------------------------------------------------------------

	@Test
	void emptyView() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder().build();
		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator();
				ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
			VectorSchemaRoot root = reader.getVectorSchemaRoot();
			reader.loadNextBatch();
			Assertions.assertThat(root.getRowCount()).isZero();
		}
	}

	// -------------------------------------------------------------------------
	// String column (VarChar)
	// -------------------------------------------------------------------------

	@Test
	void stringColumn() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("country", "FR"), Map.of("country", "DE")))
				.values(ImmutableList.of(Map.of("label", "France"), Map.of("label", "Germany")))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			Assertions.assertThat(root.getRowCount()).isEqualTo(2);

			VarCharVector country = (VarCharVector) root.getVector("country");
			Assertions.assertThat(country.getObject(0).toString()).isEqualTo("FR");
			Assertions.assertThat(country.getObject(1).toString()).isEqualTo("DE");

			VarCharVector label = (VarCharVector) root.getVector("label");
			Assertions.assertThat(label.getObject(0).toString()).isEqualTo("France");
		}
	}

	// -------------------------------------------------------------------------
	// Long (Int64)
	// -------------------------------------------------------------------------

	@Test
	void longColumn() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("ccy", "EUR")))
				.values(ImmutableList.of(Map.of("revenue", 123_456L)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			BigIntVector revenue = (BigIntVector) root.getVector("revenue");
			Assertions.assertThat(revenue.get(0)).isEqualTo(123_456L);
		}
	}

	// -------------------------------------------------------------------------
	// Integer (Int32)
	// -------------------------------------------------------------------------

	@Test
	void intColumn() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("k", "a")))
				.values(ImmutableList.of(Map.of("count", 42)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			IntVector count = (IntVector) root.getVector("count");
			Assertions.assertThat(count.get(0)).isEqualTo(42);
		}
	}

	// -------------------------------------------------------------------------
	// Double (Float64)
	// -------------------------------------------------------------------------

	@Test
	void doubleColumn() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("k", "a")))
				.values(ImmutableList.of(Map.of("rate", 0.05)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			Float8Vector rate = (Float8Vector) root.getVector("rate");
			Assertions.assertThat(rate.get(0)).isEqualTo(0.05);
		}
	}

	// -------------------------------------------------------------------------
	// Boolean
	// -------------------------------------------------------------------------

	@Test
	void booleanColumn() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("k", "a"), Map.of("k", "b")))
				.values(ImmutableList.of(Map.of("flag", Boolean.TRUE), Map.of("flag", Boolean.FALSE)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			BitVector flag = (BitVector) root.getVector("flag");
			Assertions.assertThat(flag.get(0)).isEqualTo(1);
			Assertions.assertThat(flag.get(1)).isEqualTo(0);
		}
	}

	// -------------------------------------------------------------------------
	// LocalDate (Date32 / days since epoch)
	// -------------------------------------------------------------------------

	@Test
	void localDateColumn() throws IOException {
		LocalDate date = LocalDate.of(2024, 6, 15);
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("date", date)))
				.values(ImmutableList.of(Map.of("revenue", 100L)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			Schema schema = root.getSchema();
			Assertions.assertThat(schema.findField("date").getType())
					.isInstanceOf(org.apache.arrow.vector.types.pojo.ArrowType.Date.class);

			DateDayVector dateVec = (DateDayVector) root.getVector("date");
			Assertions.assertThat(dateVec.get(0)).isEqualTo((int) date.toEpochDay());
		}
	}

	// -------------------------------------------------------------------------
	// Null values
	// -------------------------------------------------------------------------

	@Test
	void nullValues() throws IOException {
		// LinkedHashMap required to store null values (ImmutableList/Map.of reject nulls)
		Map<String, Object> row0 = new LinkedHashMap<>();
		row0.put("revenue", 10L);
		Map<String, Object> row1 = new LinkedHashMap<>();
		row1.put("revenue", null);

		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("k", "a"), Map.of("k", "b")))
				.values(ImmutableList.of(row0, row1))
				.build();
		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			BigIntVector revenue = (BigIntVector) root.getVector("revenue");
			Assertions.assertThat(revenue.isNull(1)).isTrue();
		}
	}

	// -------------------------------------------------------------------------
	// Mixed types fall back to Utf8
	// -------------------------------------------------------------------------

	@Test
	void mixedTypesFallsBackToUtf8() throws IOException {
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList.of(Map.of("k", "a"), Map.of("k", "b")))
				.values(ImmutableList.of(Map.of("mixed", "hello"), Map.of("mixed", 42L)))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			FieldVector mixed = root.getVector("mixed");
			Assertions.assertThat(mixed).isInstanceOf(VarCharVector.class);
		}
	}

	// -------------------------------------------------------------------------
	// ColumnarTabularView fast path
	// -------------------------------------------------------------------------

	@Test
	void columnarViewFastPath() throws IOException {
		ColumnarTabularView view = ColumnarTabularView.builder().build();
		view.appendRow(Map.of("country", "FR"), Map.of("revenue", 100L));
		view.appendRow(Map.of("country", "DE"), Map.of("revenue", 200L));

		byte[] bytes = roundTripColumnar(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			Assertions.assertThat(root.getRowCount()).isEqualTo(2);

			VarCharVector country = (VarCharVector) root.getVector("country");
			Assertions.assertThat(country.getObject(0).toString()).isEqualTo("FR");
			Assertions.assertThat(country.getObject(1).toString()).isEqualTo("DE");

			BigIntVector revenue = (BigIntVector) root.getVector("revenue");
			Assertions.assertThat(revenue.get(0)).isEqualTo(100L);
			Assertions.assertThat(revenue.get(1)).isEqualTo(200L);
		}
	}

	// -------------------------------------------------------------------------
	// Schema order: coordinates first, then aggregates
	// -------------------------------------------------------------------------

	@Test
	void schemaOrderCoordinatesFirst() throws IOException {
		// ImmutableMap preserves insertion order by contract
		ListBasedTabularView view = ListBasedTabularView.builder()
				.coordinates(ImmutableList
						.of(ImmutableMap.<String, Object>builder().put("country", "FR").put("ccy", "EUR").build()))
				.values(ImmutableList
						.of(ImmutableMap.<String, Object>builder().put("revenue", 1L).put("cost", 2L).build()))
				.build();

		byte[] bytes = roundTrip(view);

		try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = readBatch(bytes, allocator)) {
			List<String> fieldNames = root.getSchema().getFields().stream().map(f -> f.getName()).toList();
			Assertions.assertThat(fieldNames.subList(0, 2)).containsExactly("country", "ccy");
			Assertions.assertThat(fieldNames.subList(2, 4)).containsExactly("revenue", "cost");
		}
	}
}
