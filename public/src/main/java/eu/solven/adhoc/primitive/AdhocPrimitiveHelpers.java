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
import java.math.BigInteger;
import java.util.Collection;

import com.google.common.collect.ImmutableSet;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers methods around primitive (int, long, float, double, etc).
 *
 * @author Benoit Lacelle
 */
// https://stackoverflow.com/questions/67841630/double-versus-long-for-large-numbers-in-java
@UtilityClass
@Slf4j
public class AdhocPrimitiveHelpers {

	public static boolean isLongLike(Object o) {
		if (Integer.class.isInstance(o) || Long.class.isInstance(o)) {
			return true;
		} else if (o instanceof BigInteger) {
			return true;
		} else {
			return false;
		}
	}

	public static long asLong(Object o) {
		if (o instanceof BigInteger bigInteger) {
			return bigInteger.longValueExact();
		} else {
			return ((Number) o).longValue();
		}
	}

	/**
	 * @param o
	 * @return if this can be naturally be treated as a double. An int is `doubleLike==true`.
	 */
	public static boolean isDoubleLike(Object o) {
		return Number.class.isInstance(o);
	}

	public static double asDouble(Object o) {
		return ((Number) o).doubleValue();
	}

	/**
	 * @param o
	 * @return a normalized version of the input. Typically as `long` for `long-like` and `double` for `double-like`.
	 */
	@Deprecated(since = "Unclear if this is legit")
	public static Object normalizeValue(Object o) {
		if (o == null) {
			return null;
		}
		switch (o) {
		case Integer i:
			return i.longValue();
		case Long l:
			return l;
		case BigInteger bigI:
			return bigI.longValueExact();
		case Float f:
			return f.doubleValue();
		case Double d:
			return d;
		case BigDecimal bigD:
			return bigD.doubleValue();
		default:
			return o;
		}
	}

	public static IValueProvider normalizeValueAsProvider(Object o) {
		if (o == null) {
			return IValueProvider.NULL;
		}
		switch (o) {
		case Integer i:
			return IValueProvider.setValue(i.longValue());
		case Long l:
			return IValueProvider.setValue(l.longValue());
		case BigInteger bigI:
			return IValueProvider.setValue(bigI.longValueExact());
		case Float f:
			return IValueProvider.setValue(f.doubleValue());
		case Double d:
			return IValueProvider.setValue(d.doubleValue());
		case BigDecimal bigD:
			return IValueProvider.setValue(bigD.doubleValue());
		default:
			return IValueProvider.setValue(o);
		}
	}

	public static ImmutableSet<?> normalizeValues(Collection<?> coordinates) {
		ImmutableSet.Builder<Object> builder = ImmutableSet.builderWithExpectedSize(coordinates.size());

		coordinates.forEach(rawValue -> builder.add(normalizeValue(rawValue)));

		return builder.build();
	}

}
