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
package eu.solven.adhoc.engine.tabular.groupingset;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.IAliasedAggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.Builder;

/**
 * A decorating {@link IMultitypeMergeableGrid} which handle receiving a stream of {@link ISlice} for differing
 * {@link IGroupBy}. It will delegate its work to an underlying {@link IMultitypeMergeableGrid} based on the input
 * keySet.
 *
 * @author Benoit Lacelle
 */
@Builder
public class GroupingSetMergeableGrid implements IMultitypeMergeableGrid<ISlice> {
	protected final Supplier<IMultitypeMergeableGrid<ISlice>> gridFactory;
	protected final Map<Set<String>, IMultitypeMergeableGrid<ISlice>> groupByToGrid = new ConcurrentHashMap<>();

	@Override
	public Set<String> getAggregators() {
		return groupByToGrid.values()
				.stream()
				.flatMap(grid -> grid.getAggregators().stream())
				.collect(ImmutableSet.toImmutableSet());
	}

	@Override
	public IOpenedSlice openSlice(ISlice key) {
		return getGroupByGrid(gridFactory, key.columnsKeySet()).openSlice(key);
	}

	@Override
	public IMultitypeColumnFastGet<ISlice> closeColumn(CubeQueryStep queryStep, IAliasedAggregator aggregator) {
		return getGroupByGrid(gridFactory, queryStep.getGroupBy().getGroupedByColumns()).closeColumn(queryStep,
				aggregator);
	}

	protected IMultitypeMergeableGrid<ISlice> getGroupByGrid(Supplier<IMultitypeMergeableGrid<ISlice>> gridFactory,
			Set<String> set) {
		return groupByToGrid.computeIfAbsent(set, k -> gridFactory.get());
	}

	@Override
	public long size(String aggregator) {
		return groupByToGrid.values().stream().mapToLong(grid -> grid.size(aggregator)).sum();
	}
}
