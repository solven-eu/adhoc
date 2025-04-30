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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ezylang.evalex.EvaluationException;
import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.parser.ParseException;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Enable expression-based {@link ICombination}
 *
 * @author Benoit Lacelle
 * @see <a href="https://github.com/ezylang/EvalEx">EvalEx</a>
 */
@RequiredArgsConstructor
public class ExpressionCombination implements ICombination {
	public static final String KEY = "EXPRESSION";

	public static final String KEY_EXPRESSION = "expression";

	@NonNull
	final String expression;
	@NonNull
	final List<String> underlyingNames;

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		Expression exp = new Expression(expression);

		EvaluationValue result;
		try {
			Map<String, Object> nameToValue = new LinkedHashMap<>();

			// Enable referring to underlyings by their index
			// BEWARE This may be overriden if an underlying measure is named `underlyings`
			nameToValue.put("underlyings", underlyingValues);

			for (int i = 0; i < Math.min(underlyingNames.size(), underlyingValues.size()); i++) {
				nameToValue.put(underlyingNames.get(i), underlyingValues.get(i));
			}

			result = exp.withValues(nameToValue).evaluate();

		} catch (EvaluationException | ParseException e) {
			throw new IllegalArgumentException("Issue with expression=`%s`".formatted(expression), e);
		}

		// BEWARE In some cases, we may like to cast to long if the result is a perfect long
		if (result.isNumberValue()) {
			BigDecimal bigDecimal = result.getNumberValue();

			if (AdhocPrimitiveHelpers.isLongLike(bigDecimal)) {
				return bigDecimal.longValueExact();
			} else {
				return bigDecimal.doubleValue();
			}
		} else {
			return result.getValue();
		}
	}

	public static ExpressionCombination parse(Map<String, ?> options) {
		String expression = MapPathGet.getRequiredString(options, KEY_EXPRESSION);
		List<String> underlyingIndexToName = MapPathGet.getRequiredAs(options, IHasCombinationKey.KEY_UNDERLYING_NAMES);
		return new ExpressionCombination(expression, underlyingIndexToName);
	}

}
