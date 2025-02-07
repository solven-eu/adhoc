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
package eu.solven.adhoc.measure.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnatorQueryStep extends CombinatorQueryStep {
	final Columnator columnator;

	public ColumnatorQueryStep(Columnator columnator, IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		super(columnator, transformationFactory, step);

		this.columnator = columnator;
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		Optional<String> optMissingColumn = columnator.getRequiredColumns().stream().filter(this::isMissing).findAny();

		if (optMissingColumn.isPresent()) {
			return Collections.emptyList();
		}

		return super.getUnderlyingSteps();
	}

	private boolean isMissing(String c) {
		return !step.getGroupBy().getGroupedByColumns().contains(c) && !(isMonoSelected(c, step.getFilter()));
	}

	private boolean isMonoSelected(String column, IAdhocFilter filter) {
		if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter
				&& columnFilter.getColumn().equals(column)) {
			return true;
		}

		return false;
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		ISliceToValue output = makeCoordinateToValues();

		ICombination transformation = transformationFactory.makeCombination(combinator);

		forEachDistinctSlice(underlyings, transformation, output);

		return output;
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			IAdhocSliceWithStep slice,
			ICombination combination,
			ISliceAndValueConsumer output) {
		List<Object> underlyingVs = underlyings.stream().map(storage -> {
			AtomicReference<Object> refV = new AtomicReference<>();
			AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(refV::set);

			storage.onValue(slice, consumer);

			return refV.get();
		}).collect(Collectors.toList());

		Object value = combination.combine(slice, underlyingVs);

		output.put(slice.getAdhocSliceAsMap(), value);
	}
}
