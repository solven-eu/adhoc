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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
public class ListBasedTabularView extends ATabularView implements IReadableTabularView {

	// Split into 2 lists as a List of Map.Entry is not easy to serialize
	@Default
	@Getter
	final List<Map<String, ?>> coordinates = new ArrayList<>();
	@Default
	@Getter
	final List<Map<String, ?>> values = new ArrayList<>();

	public static ListBasedTabularView load(ITabularView from) {
		int capacity = Ints.checkedCast(from.size());
		ListBasedTabularView newView = ListBasedTabularView.builder()
				.coordinates(new ArrayList<>(capacity))
				.values(new ArrayList<>(capacity))
				.build();

		return load(from, newView);
	}

	public static ListBasedTabularView load(ITabularView from, ListBasedTabularView to) {
		if (from instanceof ListBasedTabularView asMapBased) {
			return asMapBased;
		}

		IColumnScanner<IAdhocSlice> rowScanner = coordinates -> {
			Map<String, ?> coordinatesAsMap = coordinates.getCoordinates();

			return o -> {
				Map<String, ?> oAsMap = (Map<String, ?>) o;

				to.coordinates.add(coordinatesAsMap);
				to.values.add(oAsMap);
			};
		};

		from.acceptScanner(rowScanner);

		return to;
	}

	@Override
	public Stream<IAdhocSlice> slices() {
		return coordinates.stream().map(SliceAsMap::fromMap);
	}

	@Override
	public long size() {
		return coordinates.size();
	}

	@Override
	@JsonIgnore
	public boolean isEmpty() {
		return coordinates.isEmpty();
	}

	@Override
	public void acceptScanner(IColumnScanner<IAdhocSlice> rowScanner) {
		for (int i = 0; i < size(); i++) {
			Map<String, ?> k = coordinates.get(i);
			Map<String, ?> v = values.get(i);
			rowScanner.onKey(SliceAsMap.fromMap(k)).onObject(v);
		}
	}

	@Override
	public <U> Stream<U> stream(ITabularRecordConverter<IAdhocSlice, U> rowScanner) {
		return IntStream.range(0, Ints.checkedCast(size())).mapToObj(i -> {
			Map<String, ?> k = coordinates.get(i);
			Map<String, ?> v = values.get(i);

			return rowScanner.prepare(SliceAsMap.fromMap(k)).onMap(v);
		});
	}

	public static ListBasedTabularView empty() {
		return ListBasedTabularView.builder()
				.coordinates(Collections.emptyList())
				.values(Collections.emptyList())
				.build();
	}

	public void appendSlice(IAdhocSlice slice, Map<String, ?> mToValues) {
		coordinates.add(slice.getCoordinates());
		values.add(mToValues);
	}

	public void checkIsDistinct() {
		Set<Map<String, ?>> slices = new HashSet<>();

		for (Map<String, ?> coordinate : coordinates) {
			if (!slices.add(coordinate)) {
				throw new IllegalStateException("Multiple slices with c=%s. It is illegal in %s".formatted(coordinate,
						this.getClass().getName()));
			}
		}
	}

}
