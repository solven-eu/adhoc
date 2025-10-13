/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.data.tabular;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.primitive.IValueReceiver;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

/**
 * A simple {@link ITabularView} based on a {@link ArrayList}. It is especially useful for Jackson (de)serialization.
 * 
 * {@link MapBasedTabularView} may be simpler to manipulate.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Jacksonized
@EqualsAndHashCode(callSuper = false)
public class ListMapEntryBasedTabularView extends AListBasedTabularView implements ITabularView, IWritableTabularView {

	// Split into 2 lists as a List of Map.Entry is not easy to serialize
	@Default
	@Getter
	final List<TabularEntry> entries = new ArrayList<>();

	@JsonIgnore
	final Object2IntMap<Map<String, ?>> coordinateToIndex = new Object2IntOpenHashMap<>();

	/**
	 * Associate a slice coordinate with its measure values.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	@Jacksonized
	public static class TabularEntry {
		Map<String, ?> coordinates;
		Map<String, ?> values;
	}

	public static ListMapEntryBasedTabularView withCapacity(long expectedOutputCardinality) {
		List<TabularEntry> rawArray = new ArrayList<>(Ints.checkedCast(expectedOutputCardinality));
		return ListMapEntryBasedTabularView.builder().entries(rawArray).build();
	}

	public static ListMapEntryBasedTabularView load(IReadableTabularView from) {
		long capacity = from.size();
		ListMapEntryBasedTabularView newView = withCapacity(capacity);

		return load(from, newView);
	}

	public static <T extends ListMapEntryBasedTabularView> T load(IReadableTabularView from, T to) {
		if (to.getClass().isAssignableFrom(from.getClass())) {
			return (T) to.getClass().cast(from);
		}

		IColumnScanner<IAdhocSlice> rowScanner = coordinates -> {
			Map<String, ?> coordinatesAsMap = coordinates.getCoordinates();

			return o -> {
				Map<String, ?> oAsMap = (Map<String, ?>) o;

				to.entries.add(TabularEntry.builder().coordinates(coordinatesAsMap).values(oAsMap).build());
			};
		};

		from.acceptScanner(rowScanner);

		return to;
	}

	@Override
	public Stream<IAdhocSlice> slices() {
		return entries.stream().map(TabularEntry::getCoordinates).map(SliceAsMap::fromMap);
	}

	@Override
	public long size() {
		return entries.size();
	}

	@Override
	@JsonIgnore
	public boolean isEmpty() {
		return entries.isEmpty();
	}

	@Override
	public void acceptScanner(IColumnScanner<IAdhocSlice> rowScanner) {
		entries.forEach(entry -> {
			rowScanner.onKey(SliceAsMap.fromMap(entry.getCoordinates())).onObject(entry.getValues());
		});
	}

	@Override
	public <U> Stream<U> stream(ITabularRecordConverter<IAdhocSlice, U> rowScanner) {
		return entries.stream().map(entry -> {
			return rowScanner.prepare(SliceAsMap.fromMap(entry.getCoordinates())).onMap(entry.getValues());
		});
	}

	public static ITabularView empty() {
		return builder().entries(Collections.emptyList()).build();
	}

	public void appendSlice(IAdhocSlice slice, Map<String, ?> mToValues) {
		entries.add(TabularEntry.builder().coordinates(slice.getCoordinates()).values(mToValues).build());
	}

	@Override
	public IValueReceiver sliceFeeder(IAdhocSlice slice, String measureName, boolean materializeNull) {
		Map<String, ?> coordinates = slice.getCoordinates();
		int index = getIndexForSlice(coordinates);

		return o -> {
			if (o != null || materializeNull) {
				ensureIndexForSlice(coordinates, index);

				Map<String, Object> measureMap = (Map) entries.get(index).getValues();
				Object previousValue = measureMap.put(measureName, o);

				if (previousValue != null) {
					throw new IllegalArgumentException(
							"Conflicting value for %s m=%s %s!=%s".formatted(slice, measureName, previousValue, o));
				}
			}
		};
	}

	/**
	 * Last-minute materialization of the row, given the provisioned index
	 * 
	 * @param coordinates
	 * @param index
	 */
	protected void ensureIndexForSlice(Map<String, ?> coordinates, int index) {
		if (index >= entries.size()) {
			if (index > entries.size()) {
				// May happen in case on concurrent writes.
				throw new IllegalStateException("Writing index=%s while size=%s".formatted(index, entries.size()));
			}
			entries.add(TabularEntry.builder().coordinates(coordinates).values(new LinkedHashMap<>()).build());
		}
	}

	/**
	 * 
	 * @param coordinates
	 * @return the index at which given entry should be written, ensuring this index is materialized
	 */
	protected int getIndexForSlice(Map<String, ?> coordinates) {
		return coordinateToIndex.computeIfAbsent(coordinates, k -> coordinateToIndex.size());
	}
}
