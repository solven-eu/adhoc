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
package eu.solven.adhoc.measure.combination;

import java.util.List;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.transformator.ICombinationBinding;
import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromArray;

/**
 * An {@link ICombination} can turn a {@link List} of values (typically from {@link Combinator}) into a new value. As a
 * {@link eu.solven.adhoc.measure.model.IMeasure}, it writes into current
 * {@link eu.solven.adhoc.data.row.slice.IAdhocSlice}.
 * 
 * At least one of the `.combine` methods has to be overridden.
 *
 * @author Benoit Lacelle
 */
public interface ICombination {

	// TODO This shall become the optimal API, not to require to provide Objects
	default IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		return vc -> vc.onObject(combine(slice, slicedRecord.asList()));
	}

	/**
	 * @param slice
	 * @param underlyingValues
	 *            the underlying measures values for current slice.
	 * @return the combined result at given slice.
	 */
	default Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		SlicedRecordFromArray slicedRecord = SlicedRecordFromArray.builder().measures(underlyingValues).build();
		IValueProvider valueProvider = combine(slice, slicedRecord);

		return IValueProvider.getValue(valueProvider);
	}

	default ICombinationBinding bind(List<? extends ISliceToValue> underlyings) {
		return ICombinationBinding.NULL;
	}

}
