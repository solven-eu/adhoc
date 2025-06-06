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
package eu.solven.adhoc.measure.transformator.step;

import java.util.Collections;
import java.util.List;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.filter.AndFilter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Filtrator}
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class FiltratorQueryStep extends ATransformatorQueryStep {
	final Filtrator filtrator;
	final IOperatorsFactory transformationFactory;

	@Getter
	final CubeQueryStep step;

	public List<String> getUnderlyingNames() {
		return filtrator.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		CubeQueryStep underlyingStep = CubeQueryStep.edit(step)
				.filter(AndFilter.and(step.getFilter(), filtrator.getFilter()))
				.measure(filtrator.getUnderlying())
				.build();
		return Collections.singletonList(underlyingStep);
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException(
					"underlyings.size() == %s. It should be 1".formatted(underlyings.size()));
		}

		IMultitypeColumnFastGet<SliceAsMap> storage = makeStorage();

		forEachDistinctSlice(underlyings, new CoalesceCombination(), storage::append);

		return SliceToValue.builder().column(storage).build();
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		List<?> underlyingVs = slice.getMeasures().asList();

		Object value = combination.combine(slice.getSlice(), underlyingVs);

		SliceAsMap sliceAsMap = slice.getSlice().getAdhocSliceAsMap();
		if (isDebug()) {
			log.info("[DEBUG] Write {}={} (given {}) in {}", getMeasure().getName(), value, underlyingVs, sliceAsMap);
		}

		output.putSlice(sliceAsMap).onObject(value);
	}
}
