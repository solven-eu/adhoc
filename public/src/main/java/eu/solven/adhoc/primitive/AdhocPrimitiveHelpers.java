/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.primitive;

import java.math.BigDecimal;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers methods around primitive (int, long, float, double, etc).
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
public class AdhocPrimitiveHelpers {

	public static boolean isLongLike(Object o) {
		if (Integer.class.isInstance(o) || Long.class.isInstance(o)) {
			return true;
		} else if (o instanceof BigDecimal bigDecimal) {
			try {
				long asLong = bigDecimal.longValueExact();
				if (log.isTraceEnabled()) {
					log.trace("This is a long: {} -> {}", bigDecimal, asLong);
				}
				return true;
			} catch (ArithmeticException e) {
				log.trace("This is not a long: {}", bigDecimal, e);
			}
			return false;
		} else {
			return false;
		}
	}

	public static long asLong(Object o) {
		return ((Number) o).longValue();
	}

	/**
	 *
	 * @param o
	 * @return if this can be naturally be treated as a double. An int is `doubleLike==true`.
	 */
	public static boolean isDoubleLike(Object o) {
		return Number.class.isInstance(o);
	}

	public static double asDouble(Object o) {
		return ((Number) o).doubleValue();
	}
}
