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
package eu.solven.adhoc.dataframe.aggregating;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.dataframe.column.IMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.partitioned.IPartitioned;
import eu.solven.adhoc.dataframe.column.partitioned.PartitionedColumn;
import eu.solven.adhoc.dataframe.tabular.IMultitypeMergeableGrid;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.ICubeQueryStep;
import eu.solven.adhoc.measure.model.IAliasedAggregator;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * A tabular grid where cells can be aggregated.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
public class PartitionedMultitypeMergeableGrid<T extends Comparable<T>, K> extends AAggregatingColumns<T, K>
		implements IPartitioned<IMultitypeMergeableGrid<T>> {
	@NonNull
	@Singular
	final ImmutableList<IMultitypeMergeableGrid<T>> partitions;

	@NonNull
	@Default
	AdhocFactories factories = AdhocFactories.builder().build();

	@NonNull
	@Default
	Map<String, IMultitypeColumnFastGet<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	@Override
	public Set<String> getAggregators() {
		return aggregatorToAggregates.keySet();
	}

	@Override
	public IOpenedSlice openSlice(T key) {
		return getPartition(key).openSlice(key);
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(ICubeQueryStep queryStep, IAliasedAggregator aggregator) {
		// Always parallel: assume parallelization given we are partitioned
		List<IMultitypeColumnFastGet<T>> closed =
				map(AdhocUnsafe.adhocCpuPool, c -> c.closeColumn(queryStep, aggregator));

		return PartitionedColumn.<T>builder().partitions(closed).build();
	}

	@Override
	protected int dictionarize(T key) {
		throw new UnsupportedOperationException("Partitioning");
	}

	@Override
	protected IMultitypeColumn<K> getColumn(String aggregator) {
		throw new UnsupportedOperationException("Partitioning");
	}

	@Override
	public int getNbPartitions() {
		return partitions.size();
	}

	@Override
	public IMultitypeMergeableGrid<T> getPartition(int index) {
		return partitions.get(index);
	}

}
