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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import eu.solven.adhoc.aggregations.collection.MapAggregator;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.ValueConsumer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

@Builder
public class MapBasedTabularView implements ITabularView {
	@Default
	@Getter
	final Map<Map<String, ?>, Map<String, ?>> coordinatesToValues = new HashMap<>();

	public static MapBasedTabularView load(ITabularView output) {
		MapBasedTabularView newView = MapBasedTabularView.builder().build();

		RowScanner<Map<String, ?>> rowScanner = new RowScanner<Map<String, ?>>() {

			@Override
			public ValueConsumer onKey(Map<String, ?> coordinates) {
				if (newView.coordinatesToValues.containsKey(coordinates)) {
					throw new IllegalArgumentException("Already has value for %s".formatted(coordinates));
				}

				return AsObjectValueConsumer.consumer(o -> {
					Map<String, ?> oAsMap = (Map<String, ?>) o;

					newView.coordinatesToValues.put(coordinates, oAsMap);
				});
			}
		};

		output.acceptScanner(rowScanner);

		return newView;
	}

	@Override
	public Stream<Map<String, ?>> keySet() {
		return coordinatesToValues.keySet().stream();
	}

	@Override
	public void acceptScanner(RowScanner<Map<String, ?>> rowScanner) {
		coordinatesToValues.forEach((k, v) -> {
			rowScanner.onKey(k).onObject(v);
		});
	}

	public void append(Map<String, ?> coordinates, Map<String, ?> mToValues) {
		coordinatesToValues.merge(coordinates, mToValues, new MapAggregator<String, Object>()::aggregate);
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
