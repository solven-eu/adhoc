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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.ICompactable;
import eu.solven.adhoc.data.column.ICuboid;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.IColumnFactory;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.step.CombinatorQueryStep;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluates an induced {@link CubeQueryStep} via a row-by-row Java stream over the inducer data. This is the universal
 * fallback strategy and always produces a result.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class JavaStreamInducedEvaluator implements IInducedEvaluator {

	private final IAdhocFactories factories;

	public JavaStreamInducedEvaluator(IAdhocFactories factories) {
		this.factories = factories;
	}

	@Override
	public Optional<IMultitypeMergeableColumn<IAdhocSlice>> tryEvaluate(ICuboid inducerValues,
			CubeQueryStep inducer,
			CubeQueryStep induced,
			ISliceFilter leftoverFilter,
			IAggregation aggregation,
			Aggregator aggregator) {

		IMultitypeMergeableColumn<IAdhocSlice> inducedValues =
				prepareInducedColumn(inducer, induced, inducerValues, aggregation);

		FilterMatcher filterMatcher =
				FilterMatcher.builder().filter(leftoverFilter).onMissingColumn(FilterMatcher.failOnMissing()).build();

		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();
		boolean sameColumns = inducedColumns.equals(inducer.getGroupBy().getGroupedByColumns());

		inducerValues.stream().filter(s -> filterMatcher.match(s.getSlice())).forEach(inducerSlice -> {
			IAdhocSlice inducedSlice;
			if (sameColumns) {
				inducedSlice = inducerSlice.getSlice();
			} else {
				inducedSlice = inducerSlice.getSlice().retainAll(inducedColumns);
			}
			inducerSlice.getValueProvider().acceptReceiver(inducedValues.merge(inducedSlice));
		});

		if (inducedValues instanceof ICompactable compactable) {
			log.debug("Compacting {}", compactable);
			compactable.compact();
			log.debug("Compacted {}", compactable);
		}

		return Optional.of(inducedValues);
	}

	protected IMultitypeMergeableColumn<IAdhocSlice> prepareInducedColumn(CubeQueryStep inducer,
			CubeQueryStep induced,
			ICuboid inducerValues,
			IAggregation aggregation) {
		NavigableSet<String> inducerColumns = inducer.getGroupBy().getGroupedByColumns();
		NavigableSet<String> inducedColumns = induced.getGroupBy().getGroupedByColumns();
		boolean doesBreakSorting = breakSorting(inducerColumns, inducedColumns);

		int capacity = CombinatorQueryStep.sumSizes(ImmutableSet.of(inducerValues));
		IColumnFactory columnFactory = factories.getColumnFactory();

		if (doesBreakSorting) {
			log.debug("random-insert for {} -> {}", inducerColumns, inducedColumns);
			return columnFactory.makeColumnRandomInsertions(aggregation, capacity);
		} else {
			log.debug("sorted-insert for {} -> {}", inducerColumns, inducedColumns);
			return columnFactory.makeColumn(aggregation, capacity);
		}
	}

	/**
	 * Returns {@code true} when projecting from {@code inducer} columns to {@code induced} columns would break the
	 * natural insertion order of the output column.
	 *
	 * <p>
	 * A prefix of the inducer columns always preserves order (e.g. {@code a,b,c} → {@code a} or {@code a,b}), whereas
	 * any non-prefix projection breaks it (e.g. {@code a,b} → {@code b}).
	 */
	protected static boolean breakSorting(NavigableSet<String> inducer, NavigableSet<String> induced) {
		List<String> inducerAsList = inducer.stream().limit(induced.size()).toList();
		List<String> inducedAsList = induced.stream().toList();
		return !inducerAsList.equals(inducedAsList);
	}
}
