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
package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class UnfiltratorQueryStep implements IHasUnderlyingQuerySteps {
	final Unfiltrator unfiltrator;
	final IOperatorsFactory transformationFactory;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return unfiltrator.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		AdhocQueryStep underlyingStep = AdhocQueryStep.edit(step)
				.filter(unfilter(step.getFilter()))
				.measure(ReferencedMeasure.ref(unfiltrator.getUnderlying()))
				.build();
		return Collections.singletonList(underlyingStep);
	}

	private IAdhocFilter unfilter(IAdhocFilter filter) {
		Set<String> unfilteredColumns = unfiltrator.getUnfiltereds();

		if (filter instanceof ColumnFilter columnFilter) {
			boolean columnIsUnfiltered = unfilteredColumns.contains(columnFilter.getColumn());
			boolean inverse = unfiltrator.isInverse();

			boolean eraseFilter = columnIsUnfiltered ^ inverse;

			if (eraseFilter) {
				return IAdhocFilter.MATCH_ALL;
			} else {
				return filter;
			}
		} else if (filter instanceof AndFilter andFilter) {
			List<IAdhocFilter> unfilteredAnds = andFilter.getAnd().stream().map(f -> unfilter(f)).toList();

			return AndFilter.and(unfilteredAnds);
		} else {
			throw new UnsupportedOperationException(
					"Please file a ticket at https://github.com/solven-eu/adhoc/ to support this case: filter=%s"
							.formatted(filter));
		}
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyings.size() != 1");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		return underlyings.getFirst();
	}
}
