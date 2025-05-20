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
package eu.solven.adhoc.measure.sum;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * A {@link ICombination} which substract one measure from another.
 * 
 * @author Benoit Lacelle
 */
// https://dax.guide/op/subtraction/
@Slf4j
public class SubstractionCombination implements ICombination {

	public static final String KEY = "SUBSTRACTION";

	public static boolean isProduct(String operator) {
		return "-".equals(operator) || SubstractionCombination.KEY.equals(operator)
				|| operator.equals(SubstractionCombination.class.getName());
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		if (underlyingValues.isEmpty()) {
			return null;
		} else if (underlyingValues.size() == 1) {
			return underlyingValues.getFirst();
		}

		Object left = underlyingValues.get(0);
		Object right = underlyingValues.get(1);

		if (right == null) {
			return left;
		} else if (left == null) {
			return negate(right);
		} else {
			return substract(left, right);
		}
	}

	protected Object negate(Object right) {
		if (right == null) {
			return null;
		} else if (AdhocPrimitiveHelpers.isLongLike(right)) {
			return - AdhocPrimitiveHelpers.asLong(right);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(right)) {
			return - AdhocPrimitiveHelpers.asDouble(right);
		}
		// TODO Should we return NaN ? Should we rely on some operators ? Should we rely on some interface?
		log.warn("Unclear expected behavior when negated not numbers: {}", PepperLogHelper.getObjectAndClass(right));
		return right;
	}

	protected Object substract(Object left, Object right) {
		if (AdhocPrimitiveHelpers.isLongLike(left) && AdhocPrimitiveHelpers.isLongLike(right)) {
			return AdhocPrimitiveHelpers.asLong(left) - AdhocPrimitiveHelpers.asLong(right);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(left) && AdhocPrimitiveHelpers.isDoubleLike(right)) {
			return AdhocPrimitiveHelpers.asDouble(left) - AdhocPrimitiveHelpers.asDouble(right);
		} else {
			log.warn("Unclear expected behavior when negated not numbers: {} and {}", PepperLogHelper.getObjectAndClass(left), PepperLogHelper.getObjectAndClass(right));
			return right;
		}
	}
}
