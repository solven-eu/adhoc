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
package eu.solven.adhoc.measure.step;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.step.Shiftor.ISliceEditor;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Shiftor} is an {@link IMeasure} which is enables modifying the slice to which underlying are queried.
 * 
 * It relates with {@link Filtrator} (which AND with a hardcoded {@link IAdhocFilter}).
 * 
 * It relates with {@link Unfiltrator} (which remove some columns from the queryStep {@link IAdhocFilter}).
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class Shiftor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	@Singular
	Set<String> tags;

	@NonNull
	String underlying;

	@NonNull
	ISliceEditor sliceEditor;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new ShiftorQueryStep(this, step);
	}

	public static interface ISliceEditor {
		IAdhocFilter edit(IAdhocFilter input);
	}

	protected static IAdhocFilter shift(String column, Object value, IAdhocFilter filter) {
		if (filter.isMatchAll() || filter.isMatchNone()) {
			return ColumnFilter.isEqualTo(column, value);
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			if (columnFilter.getColumn().equals(column)) {
				return ColumnFilter.isEqualTo(column, value);
			} else {
				return columnFilter;
			}
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> operands = andFilter.getOperands();

			List<IAdhocFilter> shiftedOperands = operands.stream().map(f -> shift(column, value, f)).toList();

			return AndFilter.and(shiftedOperands);
		} else {
			throw new UnsupportedOperationException(
					"Please file a ticket at https://github.com/solven-eu/adhoc/ to support this case: filter=%s"
							.formatted(filter));
		}
	}

	public static ISliceEditor shift(String column, Object value) {
		return filter -> shift(column, value, filter);
	}

}
