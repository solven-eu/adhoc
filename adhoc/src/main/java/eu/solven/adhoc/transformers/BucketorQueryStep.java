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
package eu.solven.adhoc.transformers;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.coordinate.MapComparators;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.execute.GroupByHelpers;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class BucketorQueryStep implements IHasUnderlyingQuerySteps {
	final Bucketor bucketor;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return bucketor.getUnderlyings();
	};

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlying -> {
			AdhocQueryStep object = AdhocQueryStep.edit(step)
					.groupBy(GroupByHelpers.union(step.getGroupBy(), bucketor.getGroupBy()))
					.measure(ReferencedMeasure.builder().ref(underlying).build())
					.build();
			return object;
		}).collect(Collectors.toList());
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		MultiTypeStorage<Map<String, ?>> aggregatingView = MultiTypeStorage.<Map<String, ?>>builder().build();

		IAggregation agg = transformationFactory.makeAggregation(bucketor.getAggregationKey());
		ICombination combinator =
				transformationFactory.makeCombination(bucketor.getCombinationKey(), getCombinationOptions());

		List<String> underlyingNames = getUnderlyingNames();

		boolean debug = bucketor.isDebug() || step.isDebug();
		for (Map<String, ?> coordinate : keySet(bucketor.isDebug(), underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = combinator.combine(underlyingVs);

			if (debug) {
				Map<String, Object> underylingVsAsMap = new TreeMap<>();

				for (int i = 0; i < underlyingNames.size(); i++) {
					underylingVsAsMap.put(underlyingNames.get(i), underlyingVs.get(i));
				}

				log.info("[DEBUG] m={} Combinator={} transformed {} into {} at {}",
						bucketor.getName(),
						bucketor.getCombinationKey(),
						underylingVsAsMap,
						value,
						coordinate);
			}

			if (value != null) {
				Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), coordinate);

				if (debug) {
					log.info("[DEBUG] {} contribute {} into {}", bucketor.getName(), value, outputCoordinate);
				}

				aggregatingView.merge(outputCoordinate, value, agg);
			}
		}

		return CoordinatesToValues.builder().storage(aggregatingView).build();
	}

	private Map<String, ?> getCombinationOptions() {
		return Combinator.makeAllOptions(bucketor, bucketor.getCombinationOptions());
	}

	private Map<String, ?> queryGroupBy(IAdhocGroupBy queryGroupBy, Map<String, ?> coordinates) {
		Map<String, Object> queryCoordinates = new HashMap<>();

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			Object value = coordinates.get(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			queryCoordinates.put(groupBy, value);
		});

		return queryCoordinates;
	}

	public static Iterable<? extends Map<String, ?>> keySet(boolean debug, List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet = newSet(debug);

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}

	public static Set<Map<String, ?>> newSet(boolean debug) {
		Set<Map<String, ?>> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			Comparator<Map<String, ?>> mapComparator = MapComparators.mapComparator();
			keySet = new TreeSet<>(mapComparator);
		} else {
			keySet = new HashSet<>();
		}
		return keySet;
	}
}
