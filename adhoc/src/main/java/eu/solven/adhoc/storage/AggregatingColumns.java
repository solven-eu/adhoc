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
package eu.solven.adhoc.storage;

import java.util.HashMap;
import java.util.Map;

import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.IAggregationCarrier.IHasCarriers;
import eu.solven.adhoc.storage.column.IMultitypeColumn;
import eu.solven.adhoc.storage.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.storage.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.storage.column.MultitypeHashColumn;
import eu.solven.adhoc.storage.column.MultitypeHashMergeableColumn;
import eu.solven.adhoc.storage.column.MultitypeNavigableColumn;
import lombok.Value;

/**
 * A data-structure associating each {@link Aggregator} with a {@link IMultitypeMergeableColumn}
 * 
 * @param <T>
 */
@Value
public class AggregatingColumns<T extends Comparable<T>> implements IMultitypeMergeableGrid<T> {
	// The table can not do aggregations, so Adhoc does the aggregation
	Map<Aggregator, IMultitypeMergeableColumn<T>> aggregatorToRawAggregated = new HashMap<>();
	// The table can do aggregations, so Adhoc does not need to do the aggregation
	// Need to aggregate partitioned results, like from CompositeCube providing aggregates for same slice
	Map<Aggregator, IMultitypeMergeableColumn<T>> aggregatorToPreAggregated = new HashMap<>();

	IOperatorsFactory transformationFactory;

	protected IMultitypeMergeableColumn<T> makeRawColumn(IAggregation agg) {
		// Not Navigable as not all table will provide slices properly sorted (e.g. InMemoryTable)
		return MultitypeHashMergeableColumn.<T>builder().aggregation(agg).build();
	}

	// preColumn: we would not need to merge as the DB should guarantee providing distinct aggregates
	// In fact, some DB may provide aggregates, but partitioned: we may receive the same aggregate on the same slice
	// Also, even if we hit a single aggregate, it should not be returned as-is, but returns as aggregated with null
	// given the aggregation. It is typically useful to turn `BigDecimal` from DuckDb into `double`. Another
	// SumAggregation may stick to BigDecimal
	protected IMultitypeMergeableColumn<T> makePreColumn(IAggregation agg) {
		// Not Navigable as not all table will provide slices properly sorted (e.g. InMemoryTable)
		return MultitypeHashMergeableColumn.<T>builder().aggregation(agg).build();
	}

	@Override
	public void contributeRaw(Aggregator aggregator, T key, Object v) {
		contributeRaw(aggregator, key).onObject(v);
	}

	@Override
	public IValueConsumer contributeRaw(Aggregator aggregator, T key) {
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		IMultitypeMergeableColumn<T> column =
				aggregatorToRawAggregated.computeIfAbsent(aggregator, k -> makeRawColumn(agg));

		return column.merge(key);
	}

	@Override
	public void contributePre(Aggregator aggregator, T key, Object v) {
		contributePre(aggregator, key).onObject(v);
	}

	@Override
	public IValueConsumer contributePre(Aggregator aggregator, T key) {
		String aggregationKey = aggregator.getAggregationKey();
		IAggregation agg = transformationFactory.makeAggregation(aggregationKey);

		IMultitypeColumn<T> column = aggregatorToPreAggregated.computeIfAbsent(aggregator, k -> makePreColumn(agg));

		return v -> {
			if (agg instanceof IHasCarriers hasCarriers) {
				// Wrap the aggregate from table into the aggregation custom wrapper
				v = hasCarriers.wrap(v);
			}

			column.append(key).onObject(v);
		};
	}

	public long size(Aggregator aggregator) {
		long size = 0L;

		IMultitypeColumn<T> rawColumn = aggregatorToRawAggregated.get(aggregator);
		if (rawColumn != null) {
			size += rawColumn.size();
		}

		IMultitypeColumn<T> preColumn = aggregatorToPreAggregated.get(aggregator);
		if (preColumn != null) {
			size += preColumn.size();
		}

		return size;
	}

	@Override
	public IMultitypeColumnFastGet<T> closeColumn(Aggregator aggregator) {
		IMultitypeColumnFastGet<T> rawColumn = aggregatorToRawAggregated.get(aggregator);
		IMultitypeColumnFastGet<T> preColumn = aggregatorToPreAggregated.get(aggregator);

		IMultitypeColumnFastGet<T> column;
		if (preColumn != null) {
			column = preColumn;

			if (rawColumn != null) {
				throw new IllegalStateException("Did not expected both a raw and a pre for a=%s".formatted(aggregator));
			}
		} else {
			column = rawColumn;
		}

		if (column == null) {
			// Typically happens when a filter reject completely one of the underlying
			// measure
			column = MultitypeHashColumn.empty();
		} else {
			// Typically converts a CountHolder into the count as a `long`
			column.purgeAggregationCarriers();
		}

		if (!(column instanceof MultitypeNavigableColumn<?>)) {
			// TODO Should we have some sort of strategy to decide when to switch to the sorting strategy?
			column = MultitypeNavigableColumn.copy(column);
		}

		return column;
	}
}