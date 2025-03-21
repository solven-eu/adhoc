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
package eu.solven.adhoc.measure.transformator;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class UnfiltratorQueryStep implements ITransformator {
	final Unfiltrator unfiltrator;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return unfiltrator.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.filter(unfilter(step.getFilter()))
				.measureNamed(unfiltrator.getUnderlying())
				.build();
		return Collections.singletonList(underlyingStep);
	}

	protected IAdhocFilter unfilter(IAdhocFilter filter) {
		Set<String> unfilteredColumns = unfiltrator.getUnfiltereds();

		if (filter instanceof IColumnFilter columnFilter) {
			boolean columnIsUnfiltered = unfilteredColumns.contains(columnFilter.getColumn());
			boolean inverse = unfiltrator.isInverse();

			// If inverse is false, we drop columnFilters on given columns
			// If inverse is true, we keep columnFilters on given columns
			boolean eraseFilter = columnIsUnfiltered ^ inverse;

			if (eraseFilter) {
				return IAdhocFilter.MATCH_ALL;
			} else {
				return filter;
			}
		} else if (filter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> unfilteredAnds = andFilter.getOperands().stream().map(this::unfilter).toList();

			return AndFilter.and(unfilteredAnds);
		} else {
			throw new NotYetImplementedException("filter=%s".formatted(filter));
		}
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyings.size() != 1");
		}

		// Return the column without any change, as we just edited the filter (but not the groupBy)
		return underlyings.getFirst();
	}
}
