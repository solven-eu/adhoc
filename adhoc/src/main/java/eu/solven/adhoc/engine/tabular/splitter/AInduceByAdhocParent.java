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

import java.util.Collection;
import java.util.Optional;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds generic behavior to {@link ITableStepsSplitter} which leads to steps being induced by Adhoc.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class AInduceByAdhocParent implements ITableStepsSplitter {

	protected TableQueryStep contextOnly(TableQueryStep inducer) {
		return TableQueryStep.edit(inducer)
				.aggregator(Aggregator.sum("noMeasure"))
				.groupBy(IGroupBy.GRAND_TOTAL)
				.filter(ISliceFilter.MATCH_ALL)
				.build();
	}

	// Typically: `groupBy:ccy+country;ccy=EUR|USD` can induce `ccy=EUR`
	// BEWARE This design prevents having an induced inferred by multiple inducers
	// (e.g. `WHERE A` and `WHERE B` can induce `WHERE A OR B`)
	protected boolean canInduce(TableQueryStep inducer, TableQueryStep induced) {
		if (!inducer.getMeasure().getName().equals(induced.getMeasure().getName())) {
			// Different measures: can not induce
			return false;
		} else if (!contextOnly(inducer).equals(contextOnly(induced))) {
			// Different options/customMarker: can not induce
			return false;
		}

		// BEWARE a given name may refer to a ReferencedColumn, or to a StaticCoordinateColumn (or anything else)
		Collection<IAdhocColumn> inducerColumns = inducer.getGroupBy().getColumns();
		Collection<IAdhocColumn> inducedColumns = induced.getGroupBy().getColumns();
		if (!inducerColumns.containsAll(inducedColumns)) {
			// Not expressing all needed columns: can not induce
			// If right has all groupBy of left, it means right has same or more groupBy than left,
			// hence right can be used to compute left
			return false;
		}

		ISliceFilter inducedFilter = induced.getFilter();
		if (ISliceFilter.MATCH_NONE.equals(inducedFilter)) {
			// Keep matchNone node aside: they do not need to wait for any other node to be evaluated
			return false;
		}

		ISliceFilter inducerFilter = inducer.getFilter();

		if (!FilterHelpers.isStricterThan(inducedFilter, inducerFilter)) {
			// Induced is not covered by inducer: it can not infer it
			return false;
		}

		Optional<ISliceFilter> leftoverFilter =
				InducerHelpers.makeLeftoverFilter(inducerColumns, inducerFilter, inducedFilter);

		if (leftoverFilter.isEmpty()) {
			// Inducer is missing columns to reject rows not expected by induced
			return false;
		}

		return true;
	}

}
