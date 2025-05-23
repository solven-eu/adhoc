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

import java.util.List;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.combination.IHasTwoOperands;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ICombination} which substract one measure from another.
 * 
 * @author Benoit Lacelle
 */
// https://dax.guide/op/subtraction/
@Slf4j
public class SubstractionCombination implements ICombination, IHasTwoOperands {

	public static final String KEY = "SUBSTRACTION";

	public static boolean isSubstraction(String operator) {
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

	protected Object negate(Object o) {
		if (o == null) {
			return null;
		} else if (AdhocPrimitiveHelpers.isLongLike(o)) {
			return -AdhocPrimitiveHelpers.asLong(o);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(o)) {
			return -AdhocPrimitiveHelpers.asDouble(o);
		}

		if (AdhocUnsafe.failFast) {
			throw new NotYetImplementedException("Unclear expected behavior when negating not numbers: %s"
					.formatted(PepperLogHelper.getObjectAndClass(o)));
		} else {
			// TODO Should we return NaN ? Should we rely on some operators ? Should we rely on some interface?
			log.warn("Unclear expected behavior when negating not numbers: {}", PepperLogHelper.getObjectAndClass(o));
			return o;
		}
	}

	protected Object substract(Object left, Object right) {
		if (AdhocPrimitiveHelpers.isLongLike(left) && AdhocPrimitiveHelpers.isLongLike(right)) {
			return AdhocPrimitiveHelpers.asLong(left) - AdhocPrimitiveHelpers.asLong(right);
		} else if (AdhocPrimitiveHelpers.isDoubleLike(left) && AdhocPrimitiveHelpers.isDoubleLike(right)) {
			return AdhocPrimitiveHelpers.asDouble(left) - AdhocPrimitiveHelpers.asDouble(right);
		} else {
			if (AdhocUnsafe.failFast) {
				throw new NotYetImplementedException(
						"Unclear expected behavior when substracting not numbers: %s and %s".formatted(
								PepperLogHelper.getObjectAndClass(left),
								PepperLogHelper.getObjectAndClass(right)));
			} else {
				log.warn("Unclear expected behavior when substracting not numbers: {} and {}",
						PepperLogHelper.getObjectAndClass(left),
						PepperLogHelper.getObjectAndClass(right));
				return left + " - " + right;
			}
		}
	}
}
