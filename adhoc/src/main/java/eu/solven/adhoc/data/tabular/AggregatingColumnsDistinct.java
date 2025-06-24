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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import eu.solven.adhoc.util.AdhocUnsafe;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A data-structure associating each {@link Aggregator} with a {@link IMultitypeMergeableColumn}
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@SuppressWarnings("checkstyle:TypeName")
public class AggregatingColumnsDistinct<T extends Comparable<T>> extends AAggregatingColumns<T, Integer> {
	@NonNull
	@Default
	AdhocFactories factories = AdhocFactories.builder().build();

	// May go for Hash or Navigable
	// This dictionarize the slice, with a common dictionary to all aggregators. This is expected to be efficient as, in
	// most cases, all aggregators covers the same slices. But this design enables sorting the slices only once (for all
	// aggregators).
	@NonNull
	@Default
	List<T> indexToSlice = new ArrayList<>(AdhocUnsafe.getDefaultColumnCapacity());

	@NonNull
	@Default
	Map<String, IMultitypeColumnFastGet<Integer>> aggregatorToAggregates = new LinkedHashMap<>();

	// preColumn: we would not need to merge as the DB should guarantee providing distinct aggregates
	// In fact, some DB may provide aggregates, but partitioned: we may receive the same aggregate on the same slice
	// Also, even if we hit a single aggregate, it should not be returned as-is, but returns as aggregated with null
	// given the aggregation. It is typically useful to turn `BigDecimal` from DuckDb into `double`. Another
	// SumAggregation may stick to BigDecimal
	protected IMultitypeColumnFastGet<Integer> makePreColumn() {
		// Not all table will provide slices properly sorted (e.g. InMemoryTable)
		return factories.getColumnsFactory().makeColumn(Arrays.asList());
	}

	@Override
	protected IMultitypeColumnFastGet<Integer> getColumn(IAliasedAggregator aggregator) {
		return aggregatorToAggregates.get(aggregator.getAlias());
	}

	@Override
	public IOpenedSlice openSlice(T key) {
		int keyIndex = dictionarize(key);

		return aggregator -> {
			IMultitypeColumnFastGet<Integer> column =
					aggregatorToAggregates.computeIfAbsent(aggregator.getAlias(), k -> makePreColumn());

			// TODO Could be skipped for not-object aggregate?
			IAggregation agg = operatorFactory.makeAggregation(aggregator.getAggregator());

			if (agg instanceof IHasCarriers hasCarriers) {
				return hasCarriers.wrap(column.append(keyIndex));
			} else {
				return column.append(keyIndex);
			}
		};
	}

	@Override
	protected int dictionarize(T key) {
		indexToSlice.add(key);

		// e.g. On first item, we return 0
		return indexToSlice.size() - 1;
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(IAliasedAggregator aggregator) {
		IMultitypeColumnFastGet<Integer> notFinalColumn = getColumn(aggregator);

		if (notFinalColumn == null) {
			// Typically happens when a filter reject completely one of the underlying
			// measure, and not a single aggregate was written
			notFinalColumn = MultitypeHashColumn.empty();
		}

		IMultitypeColumnFastGet<Integer> column = notFinalColumn;

		// Reverse from `slice->index` to `index->slice`
		Object2IntMap<T> sliceToIndex = new Object2IntOpenHashMap<>(indexToSlice.size());
		for (int i = 0; i < indexToSlice.size(); i++) {
			sliceToIndex.put(indexToSlice.get(i), i);
		}

		// Turn the columnByIndex to a columnBySlice
		return AggregatingColumns.undictionarizeColumn(column, sliceToIndex::getInt, indexToSlice::get);
	}

}