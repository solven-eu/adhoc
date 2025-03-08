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
package eu.solven.adhoc.dag;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.record.IAggregatedRecord;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.InMemoryTable;
import lombok.Builder;
import lombok.Value;

/**
 * This is typically useful to manage cases where an {@link IAdhocTableWrapper} is not able to compute an aggregate, and
 * the aggregation has to be done by IAdhoc.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class TableAggregatesMetadata {
	// a mapping from measureName to aggregator when the aggregates are evaluated by the table
	final Map<String, Aggregator> measureToPre;

	// mapping from table columns to the aggregators when the aggregates are evaluated by Adhoc
	final SetMultimap<String, Aggregator> columnToRaw;

	// set of tables measures. They may not be only actual measures if `columnToRaw` is not empty
	final Supplier<Set<String>> names = Suppliers
			.memoize(() -> Stream.concat(getMeasureToPre().keySet().stream(), getColumnToRaw().keySet().stream())
					.collect(ImmutableSet.toImmutableSet()));

	/**
	 * 
	 * @return the {@link List} of names expected in {@link IAggregatedRecord}
	 */
	public Set<String> getMeasures() {
		return names.get();
	}

	/**
	 * 
	 * @param aggregatedMeasure
	 * @return the {@link Aggregator} associated to given measureName, when Adhoc is receiving aggregates
	 */
	public Aggregator getAggregation(String aggregatedMeasure) {
		return measureToPre.get(aggregatedMeasure);
	}

	/**
	 * 
	 * @param aggregatedMeasure
	 * @return the {@link Aggregator} associated to given column, when Adhoc is responsible for aggregation.
	 */
	public Set<Aggregator> getRaw(String aggregatedMeasure) {
		return columnToRaw.get(aggregatedMeasure);
	}

	public static TableAggregatesMetadata from(ExecutingQueryContext executingQueryContext,
			SetMultimap<String, Aggregator> columnToAggregators) {
		Map<String, Aggregator> nameToPre;
		SetMultimap<String, Aggregator> nameToRaw;

		if (executingQueryContext.getTable() instanceof InMemoryTable) {
			// We may receive raw columns,to be aggregated by ourselves
			ImmutableSetMultimap.Builder<String, Aggregator> nameToRawBuilder = ImmutableSetMultimap.builder();

			columnToAggregators.values().stream().forEach(a -> nameToRawBuilder.putAll(a.getColumnName(), a));
			nameToRaw = nameToRawBuilder.build();

			nameToPre = new LinkedHashMap<>();
		} else {
			// We may also receive pre-aggregated columns

			// We consider all other tables can do all aggregations
			// BEWARE What if a table would not be able to do only a subset of aggregations?
			nameToPre = new LinkedHashMap<>();
			columnToAggregators.values().forEach(a -> nameToPre.put(a.getName(), a));

			nameToRaw = ImmutableSetMultimap.of();
		}

		return TableAggregatesMetadata.builder().measureToPre(nameToPre).columnToRaw(nameToRaw).build();
	}

}
