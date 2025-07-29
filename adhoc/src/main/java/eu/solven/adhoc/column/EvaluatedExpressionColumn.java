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
package eu.solven.adhoc.column;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ezylang.evalex.EvaluationException;
import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.parser.ParseException;

import eu.solven.adhoc.data.row.ITabularGroupByRecord;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * A {@link EvaluatedExpressionColumn} is a column which is calculated given an expression, by Adhoc engine, not by the
 * {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class EvaluatedExpressionColumn implements IAdhocColumn, ICalculatedColumn {
	// The name of the evaluated column
	@NonNull
	String name;

	@NonNull
	@Default
	Class<?> type = Object.class;

	@NonNull
	String expression;

	/**
	 * BEWARE This must not be cached as {@link Expression} is a stateful object
	 * (https://github.com/ezylang/EvalEx/issues/83).
	 * 
	 * @return an Expression object
	 */
	protected Expression makeExpression() {
		return new Expression(expression);
	}

	@Override
	public Object computeCoordinate(ITabularGroupByRecord record) {
		Expression exp = makeExpression();

		EvaluationValue result;
		try {
			Map<String, Object> nameToValue = new LinkedHashMap<>();

			exp.getUsedVariables().forEach(variable -> {
				exp.with(variable, record.getGroupBy(variable));
			});

			result = exp.withValues(nameToValue).evaluate();
		} catch (EvaluationException | ParseException e) {
			throw new IllegalArgumentException("Issue with expression=`%s`".formatted(expression), e);
		}

		return result.getValue();
	}

	@Override
	public Class<?> getType() {
		return type;
	}

}
