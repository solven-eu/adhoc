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
package eu.solven.adhoc.measure.transformator;

import java.util.Arrays;

import eu.solven.adhoc.primitive.IValueProvider;
import lombok.experimental.UtilityClass;

/**
 * Utilities around debug operations. These methods may be slow (or just *inefficient*), which is acceptable for
 * debugging purposes.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocDebug {

	public static String toString(Object o) {
		if (o instanceof int[] array) {
			return Arrays.toString(array);
		} else if (o instanceof long[] array) {
			return Arrays.toString(array);
		} else if (o instanceof float[] array) {
			return Arrays.toString(array);
		} else if (o instanceof double[] array) {
			return Arrays.toString(array);
		} else if (o instanceof Object[] array) {
			return Arrays.deepToString(array);
		} else if (o instanceof IValueProvider valueProvider) {
			return toString(IValueProvider.getValue(valueProvider));
		} else {
			return String.valueOf(o);
		}
	}

}
