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
package eu.solven.adhoc;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import eu.solven.adhoc.aggregations.collection.MapAggregator;
import eu.solven.adhoc.coordinate.MapComparators;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.IAdhocSlice;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

/**
 * A simple {@link ITabularView} based on a {@link TreeMap}. it is especially useful for debugging purposes.
 */
@Builder
public class MapBasedTabularView implements ITabularView {
	@Default
	@Getter
	final Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = new TreeMap<>(MapComparators.mapComparator());

	public static MapBasedTabularView load(ITabularView from) {
		MapBasedTabularView newView = MapBasedTabularView.builder().build();

		return load(from, newView);
	}

	public static MapBasedTabularView load(ITabularView from, MapBasedTabularView to) {
		if (from instanceof MapBasedTabularView asMapBased) {
			return asMapBased;
		}

		RowScanner<IAdhocSlice> rowScanner = coordinates -> {
			Map<String, Object> coordinatesAsMap = coordinates.getCoordinates();

			return AsObjectValueConsumer.consumer(o -> {
				Map<String, ?> oAsMap = (Map<String, ?>) o;

				Object previousValue = to.coordinatesToValues.put(coordinatesAsMap, oAsMap);
				if (previousValue != null) {
					throw new IllegalArgumentException("Already has value for %s".formatted(coordinates));
				}
			});
		};

		from.acceptScanner(rowScanner);

		return to;
	}

	@Override
	public Stream<IAdhocSlice> slices() {
		return coordinatesToValues.keySet().stream().map(AdhocSliceAsMap::fromMap);
	}

	@Override
	public int size() {
		return coordinatesToValues.size();
	}

	@Override
	public boolean isEmpty() {
		return coordinatesToValues.isEmpty();
	}

	@Override
	public void acceptScanner(RowScanner<IAdhocSlice> rowScanner) {
		coordinatesToValues.forEach((k, v) -> {
			rowScanner.onKey(AdhocSliceAsMap.fromMap(k)).onObject(v);
		});
	}

	public void appendSlice(AdhocSliceAsMap slice, Map<String, ?> mToValues) {
		coordinatesToValues.merge(slice.getCoordinates(), mToValues, MapAggregator::aggregateMaps);
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
				.limit(5)
				.forEach(entry -> toStringHelper.add("#" + index.getAndIncrement(), entry));

		return toStringHelper.toString();
	}
}
