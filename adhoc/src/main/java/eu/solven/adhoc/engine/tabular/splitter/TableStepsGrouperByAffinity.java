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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.table.TableQueryV3;

/**
 * Groups {@link CubeQueryStep} inducers via greedy biclique decomposition of the {@code (measure+filter, groupBy)}
 * bipartite graph. Each produced {@link TableQueryV3} covers exactly the required {@code (measure, groupBy)}
 * combinations, eliminating the cartesian-product waste that arises when unrelated measures and groupBys are mixed in a
 * single GROUPING SETS query.
 *
 * <p>
 * Algorithm (per mandatory context group):
 * <ol>
 * <li>Build a bipartite graph: left nodes = {@code (measure, filter)} pairs; right nodes = {@code IGroupBy} values;
 * edges = one per {@link CubeQueryStep}.</li>
 * <li>Repeatedly pick the left node with the most remaining right neighbours, expand the biclique by retaining only
 * left nodes that cover <em>all</em> of those right neighbours, emit the corresponding steps as a group, and remove the
 * covered edges.</li>
 * <li>Repeat until no edges remain.</li>
 * </ol>
 *
 * <p>
 * Worst case: one group per step (equivalent to {@link TableStepsGrouperNoGroup}). Best case: all steps fit in one
 * biclique (equivalent to {@link TableStepsGrouper}).
 *
 * @author Benoit Lacelle
 */
public class TableStepsGrouperByAffinity extends TableStepsGrouper {

	/**
	 * Left node in the bipartite graph: the {@code (measure, step-level filter)} pair that identifies a unique
	 * {@code FilteredAggregator} in the resulting {@link TableQueryV3}.
	 */
	private record LeftKey(IMeasure measure, ISliceFilter filter) {
	}

	@Override
	public Collection<? extends Collection<CubeQueryStep>> groupInducers(Set<CubeQueryStep> inducers) {
		// Mandatory partition: steps with different (options, customMarker) must be in separate TableQueryV3
		Map<CubeQueryStep, List<CubeQueryStep>> mandatoryGroups = inducers.stream()
				.collect(Collectors.groupingBy(this::tableQueryGroupBy, LinkedHashMap::new, Collectors.toList()));

		List<Collection<CubeQueryStep>> result = new ArrayList<>();
		mandatoryGroups.values().forEach(group -> result.addAll(bicliqueCover(group)));
		return result;
	}

	/**
	 * Partitions {@code steps} (which all share the same mandatory context) into groups whose {@code (measure+filter,
	 * groupBy)} pairs form a complete bipartite subgraph, so that the resulting {@link TableQueryV3} contains no
	 * unrequested combinations.
	 */
	protected List<Set<CubeQueryStep>> bicliqueCover(List<CubeQueryStep> steps) {
		Set<CubeQueryStep> remaining = new LinkedHashSet<>(steps);
		List<Set<CubeQueryStep>> groups = new ArrayList<>();

		while (!remaining.isEmpty()) {
			// Build left-node → right-nodes adjacency for the remaining steps
			Map<LeftKey, Set<IGroupBy>> leftNeighbors = new LinkedHashMap<>();
			for (CubeQueryStep step : remaining) {
				leftNeighbors.computeIfAbsent(leftKey(step), k -> new LinkedHashSet<>()).add(step.getGroupBy());
			}

			// Pick the left node with the most right neighbours (greedy seed)
			LeftKey bestLeft = leftNeighbors.entrySet()
					.stream()
					.max(Comparator.comparingInt(e -> e.getValue().size()))
					.get()
					.getKey();
			Set<IGroupBy> bestGroupBys = leftNeighbors.get(bestLeft);

			// Expand the biclique: keep only left nodes whose neighbours are a superset of bestGroupBys
			Set<LeftKey> compatibleLefts = leftNeighbors.entrySet()
					.stream()
					.filter(e -> e.getValue().containsAll(bestGroupBys))
					.map(Map.Entry::getKey)
					.collect(Collectors.toCollection(LinkedHashSet::new));

			// Collect the steps that belong to this biclique
			Set<CubeQueryStep> group = new LinkedHashSet<>();
			for (CubeQueryStep step : remaining) {
				if (compatibleLefts.contains(leftKey(step)) && bestGroupBys.contains(step.getGroupBy())) {
					group.add(step);
				}
			}

			groups.add(group);
			remaining.removeAll(group);
		}

		return groups;
	}

	private LeftKey leftKey(CubeQueryStep step) {
		return new LeftKey(step.getMeasure(), step.getFilter());
	}

}
