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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.MapComparators;
import eu.solven.adhoc.measure.aggregation.collection.MapAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link ITabularView} based on a {@link TreeMap}. it is especially useful for debugging purposes.
 * 
 * This is NOT serializable with Jackson. {@link ListBasedTabularView} is a serializable alternative.
 * 
 * @author Benoit Lacelle
 */
@Builder
@EqualsAndHashCode
@Slf4j
public class MapBasedTabularView implements ITabularView {
	@Default
	@Getter
	final Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = new TreeMap<>(MapComparators.mapComparator());

	public static MapBasedTabularView load(ITabularView from) {
		int capacity = Ints.checkedCast(from.size());
		Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = LinkedHashMap.newLinkedHashMap(capacity);
		MapBasedTabularView newView = MapBasedTabularView.builder().coordinatesToValues(coordinatesToValues).build();

		return load(from, newView);
	}

	public static MapBasedTabularView load(ITabularView from, MapBasedTabularView to) {
		if (from instanceof MapBasedTabularView asMapBased) {
			return asMapBased;
		}

		IColumnScanner<IAdhocSlice> rowScanner = coordinates -> {
			Map<String, ?> coordinatesAsMap = coordinates.getCoordinates();

			return o -> {
				Map<String, ?> oAsMap = (Map<String, ?>) o;

				Object previousValue = to.coordinatesToValues.put(coordinatesAsMap, oAsMap);
				if (previousValue != null) {
					throw new IllegalArgumentException("Already has value for %s".formatted(coordinates));
				}
			};
		};

		from.acceptScanner(rowScanner);

		return to;
	}

	@Override
	public Stream<IAdhocSlice> slices() {
		return coordinatesToValues.keySet().stream().map(SliceAsMap::fromMap);
	}

	@Override
	public long size() {
		return coordinatesToValues.size();
	}

	@Override
	public boolean isEmpty() {
		return coordinatesToValues.isEmpty();
	}

	@Override
	public void acceptScanner(IColumnScanner<IAdhocSlice> rowScanner) {
		coordinatesToValues.forEach((k, v) -> {
			rowScanner.onKey(SliceAsMap.fromMap(k)).onObject(v);
		});
	}

	@Override
	public <U> Stream<U> stream(ITabularRecordConverter<IAdhocSlice, U> rowScanner) {
		return coordinatesToValues.entrySet()
				.stream()
				.map(e -> rowScanner.prepare(SliceAsMap.fromMap(e.getKey())).onMap(e.getValue()));
	}

	public void appendSlice(IAdhocSlice slice, Map<String, ?> mToValues) {
		log.debug("slice={} measures={}", slice, mToValues);
		coordinatesToValues.merge(slice.getCoordinates(), mToValues, MapAggregation::aggregateMaps);
	}

	public void appendSlice(IAdhocSlice slice, String measure, Object value) {
		coordinatesToValues.merge(slice.getCoordinates(), Map.of(measure, value), MapAggregation::aggregateMaps);
	}

	public IValueReceiver sliceFeeder(IAdhocSlice slice, String measureName, boolean materializeNull) {
		return o -> {
			if (o == null) {
				// Materialize the slice. Especially useful with EmptyAggregation as defaultMeasure
				if (materializeNull) {
					appendSlice(slice, Map.of());
				}
			} else {
				appendSlice(slice, Map.of(measureName, o));
			}
		};
	}

	public static ITabularView empty() {
		return MapBasedTabularView.builder().coordinatesToValues(Collections.emptyMap()).build();
	}

	@Override
	public String toString() {
		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", coordinatesToValues.size());

		AtomicInteger index = new AtomicInteger();
		coordinatesToValues.entrySet()
				.stream()
				.limit(AdhocUnsafe.limitOrdinalToString)
				.forEach(entry -> toStringHelper.add("#" + index.getAndIncrement(), entry));

		return toStringHelper.toString();
	}

}
