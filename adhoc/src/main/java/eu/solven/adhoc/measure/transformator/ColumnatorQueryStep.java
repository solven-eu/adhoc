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
import java.util.Optional;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnatorQueryStep extends CombinatorQueryStep {
	final Columnator columnator;

	public ColumnatorQueryStep(Columnator columnator, IOperatorsFactory transformationFactory, CubeQueryStep step) {
		super(columnator, transformationFactory, step);

		this.columnator = columnator;
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		Optional<String> optMissingColumn = columnator.getColumns().stream().filter(this::isRejecting).findAny();

		if (optMissingColumn.isPresent()) {
			return Collections.emptyList();
		}

		return super.getUnderlyingSteps();
	}

	protected boolean isRejecting(String column) {
		boolean columnIsPresent = isColumnPresent(column);

		// if false, columns are checked for presence
		// if true, columns are checked for absence
		boolean inversed = columnator.isRequired();

		if (columnIsPresent && !inversed) {
			// column is present and required for presence
			return false;
		} else if (!columnIsPresent && inversed) {
			// column is missing and required for absence
			return false;
		} else {
			return true;
		}
	}

	protected boolean isColumnPresent(String column) {
		return step.getGroupBy().getGroupedByColumns().contains(column) || isMonoSelected(step.getFilter(), column);
	}

	protected boolean isMonoSelected(IAdhocFilter filter, String column) {
		IValueMatcher valueMatcher = FilterHelpers.getValueMatcher(filter, column);
		return valueMatcher instanceof EqualsMatcher;
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		IMultitypeColumnFastGet<SliceAsMap> storage = makeStorage();

		ICombination transformation = operatorsFactory.makeCombination(combinator);

		forEachDistinctSlice(underlyings, transformation, storage::append);

		return SliceToValue.builder().column(storage).build();
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			ICombination combination,
			ISliceAndValueConsumer output) {
		List<?> underlyingVs = slice.getMeasures().asList();

		Object value = combination.combine(slice.getSlice(), underlyingVs);

		output.putSlice(slice.getSlice().getAdhocSliceAsMap()).onObject(value);
	}
}
