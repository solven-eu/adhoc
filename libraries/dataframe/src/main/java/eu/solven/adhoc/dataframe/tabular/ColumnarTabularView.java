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
package eu.solven.adhoc.dataframe.tabular;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.map.AdhocMapHelpers;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * A columnar {@link IReadableTabularView}: each coordinate dimension and each aggregate measure is stored as a
 * dedicated {@link List} rather than embedding all values in per-row {@link Map}s.
 *
 * <p>
 * This layout maps directly to Apache Arrow column vectors, so {@code TabularViewArrowSerializer} can write Arrow IPC
 * data without first transposing into per-row maps.
 *
 * <p>
 * {@link #coordinateColumns} maps each dimension column name to its per-row values (insertion order preserved).
 * {@link #aggregateColumns} maps each measure name to its per-row values (insertion order preserved). Both lists for a
 * given row index {@code i} describe a single result cell.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@EqualsAndHashCode(callSuper = false)
public class ColumnarTabularView extends AListBasedTabularView implements IReadableTabularView {

	@Default
	@Getter
	final Map<String, List<?>> coordinateColumns = new LinkedHashMap<>();

	@Default
	@Getter
	final Map<String, List<?>> aggregateColumns = new LinkedHashMap<>();

	/**
	 * Converts any {@link IReadableTabularView} to a {@link ColumnarTabularView}. If the input is already a
	 * {@link ColumnarTabularView} it is returned unchanged.
	 */
	public static ColumnarTabularView load(IReadableTabularView from) {
		if (from instanceof ColumnarTabularView c) {
			return c;
		}
		return fromListBased(ListBasedTabularView.load(from));
	}

	protected static ColumnarTabularView fromListBased(ListBasedTabularView listView) {
		ColumnarTabularView result = ColumnarTabularView.builder().build();
		int size = Ints.checkedCast(listView.size());
		if (size == 0) {
			return result;
		}

		listView.getCoordinates().get(0).keySet().forEach(k -> result.coordinateColumns.put(k, new ArrayList<>(size)));
		listView.getValues().get(0).keySet().forEach(k -> result.aggregateColumns.put(k, new ArrayList<>(size)));

		for (int i = 0; i < size; i++) {
			Map<String, ?> coords = listView.getCoordinates().get(i);
			Map<String, ?> vals = listView.getValues().get(i);
			result.coordinateColumns.forEach((col, list) -> ((List<Object>) list).add(coords.get(col)));
			result.aggregateColumns.forEach((col, list) -> ((List<Object>) list).add(vals.get(col)));
		}
		return result;
	}

	/**
	 * Appends a single row. Column lists are created on first encounter of a key, consistent with incremental
	 * construction.
	 */
	public void appendRow(Map<String, ?> coords, Map<String, ?> values) {
		coords.forEach((k, v) -> ((List<Object>) coordinateColumns.computeIfAbsent(k, x -> new ArrayList<>())).add(v));
		values.forEach((k, v) -> ((List<Object>) aggregateColumns.computeIfAbsent(k, x -> new ArrayList<>())).add(v));
	}

	@Override
	public long size() {
		return coordinateColumns.values()
				.stream()
				.findFirst()
				.map(l -> (long) l.size())
				.orElseGet(() -> aggregateColumns.values().stream().findFirst().map(l -> (long) l.size()).orElse(0L));
	}

	@Override
	@JsonIgnore
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Stream<IAdhocSlice> slices() {
		List<String> coordKeys = new ArrayList<>(coordinateColumns.keySet());
		return IntStream.range(0, Ints.checkedCast(size())).mapToObj(i -> rowSlice(coordKeys, i));
	}

	@Override
	public void acceptScanner(IColumnScanner<IAdhocSlice> rowScanner) {
		List<String> coordKeys = new ArrayList<>(coordinateColumns.keySet());
		List<String> aggKeys = new ArrayList<>(aggregateColumns.keySet());
		int size = Ints.checkedCast(size());

		for (int i = 0; i < size; i++) {
			IAdhocSlice slice = rowSlice(coordKeys, i);
			Map<String, Object> aggRow = rowAggregates(aggKeys, i);
			rowScanner.onKey(slice).onObject(aggRow);
		}
	}

	@Override
	public <U> Stream<U> stream(ITabularRecordConverter<IAdhocSlice, U> rowConverter) {
		List<String> coordKeys = new ArrayList<>(coordinateColumns.keySet());
		List<String> aggKeys = new ArrayList<>(aggregateColumns.keySet());

		return IntStream.range(0, Ints.checkedCast(size())).mapToObj(i -> {
			IAdhocSlice slice = rowSlice(coordKeys, i);
			Map<String, Object> aggRow = rowAggregates(aggKeys, i);
			return rowConverter.prepare(slice).onMap(aggRow);
		});
	}

	protected IAdhocSlice rowSlice(List<String> coordKeys, int rowIndex) {
		Map<String, Object> row = new LinkedHashMap<>(coordKeys.size());
		for (String key : coordKeys) {
			row.put(key, coordinateColumns.get(key).get(rowIndex));
		}
		return AdhocMapHelpers.fromMap(sliceFactory, row).asSlice();
	}

	protected Map<String, Object> rowAggregates(List<String> aggKeys, int rowIndex) {
		Map<String, Object> row = new LinkedHashMap<>(aggKeys.size());
		for (String key : aggKeys) {
			row.put(key, aggregateColumns.get(key).get(rowIndex));
		}
		return row;
	}
}
