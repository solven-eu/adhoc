/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.beta.schema;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.Beta;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingNames;
import lombok.Builder;
import lombok.Value;

/**
 * Provided information about measures and columns which may be of interest for a User. Typically used to push them
 * forward in Pivotable.
 * 
 * @author Benoit Lacelle
 */
@Beta
public class RelevancyHeuristic {
	/**
	 * Holds the score of a heuristic. Higher is better/more relevant.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class MeasureRelevancy {
		// Higher means more relevant
		double score;
	}

	/**
	 * Holds the relevancy of the different structures of a {@link ICubeWrapper}.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class CubeRelevancy {
		Map<String, MeasureRelevancy> measureToRelevancy;
		Map<String, MeasureRelevancy> columnToRelevancy;

		public static CubeRelevancy empty() {
			return CubeRelevancy.builder().columnToRelevancy(Map.of()).measureToRelevancy(Map.of()).build();
		}
	}

	public CubeRelevancy computeRelevancies(IMeasureForest forest) {
		Set<String> leaves = new HashSet<>();
		SetMultimap<String, String> measureToDependants =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		Set<IMeasure> measures = forest.getMeasures();

		if (measures.isEmpty()) {
			return CubeRelevancy.empty();
		}

		measures.forEach(m -> {
			if (m instanceof IHasUnderlyingNames underlyings) {
				underlyings.getUnderlyingNames()
						.forEach(underlying -> measureToDependants.get(underlying).add(m.getName()));
			} else {
				leaves.add(m.getName());
			}
		});

		AtomicLongMap<String> measureToDepth = AtomicLongMap.create();
		// leaves.forEach(measureToDepth::incrementAndGet);

		AtomicLongMap<String> columnToRef = AtomicLongMap.create();

		int leavesDepth = 1;
		int depth = leavesDepth;
		Set<String> currentDepth = new LinkedHashSet<>(leaves);

		while (true) {
			int depthF = depth;
			Set<String> nextDepth = new LinkedHashSet<>();

			AtomicBoolean oneAdded = new AtomicBoolean();
			currentDepth.forEach(currentD -> {
				if (!measureToDepth.containsKey(currentD)) {
					measureToDepth.put(currentD, depthF);

					nextDepth.addAll(measureToDependants.get(currentD));

					IMeasure measure = forest.resolveIfRef(ReferencedMeasure.ref(currentD));
					if (measure instanceof Columnator columnator) {
						columnator.getColumns().forEach(columnToRef::incrementAndGet);
					} else if (measure instanceof Partitionor partitionor) {
						partitionor.getGroupBy().getGroupedByColumns().forEach(columnToRef::incrementAndGet);
					}

					oneAdded.set(true);
				}
				// else already added. May be a deep measure referring a shallow measure
			});

			if (oneAdded.get()) {
				currentDepth.clear();
				currentDepth.addAll(nextDepth);
				depth++;
			} else {
				break;
			}
		}

		long maxDepth = measureToDepth.asMap().values().stream().mapToLong(l -> l).max().getAsLong();

		Map<String, MeasureRelevancy> measureToRelevancy = new TreeMap<>();

		measureToDepth.asMap().forEach((measure, mDepth) -> {
			if (mDepth == leavesDepth) {
				// Leaf measure, generally aggregated by the ITableWrapper. They are often meaningful for users, as they
				// know about the table model more then the tree of measures.
				measureToRelevancy.put(measure, MeasureRelevancy.builder().score(mDepth).build());
			} else if (mDepth == maxDepth) {
				// The most abstract measure are often a good start for user
				measureToRelevancy.put(measure, MeasureRelevancy.builder().score(mDepth).build());
			}
		});

		Map<String, MeasureRelevancy> columnToRelevancy = new TreeMap<>();
		columnToRef.asMap().forEach((column, nbRef) -> {
			columnToRelevancy.put(column, MeasureRelevancy.builder().score(nbRef).build());
		});

		return CubeRelevancy.builder()
				.measureToRelevancy(measureToRelevancy)
				.columnToRelevancy(columnToRelevancy)
				.build();
	}
}
