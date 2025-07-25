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
package eu.solven.adhoc.measure.decomposition;

import java.util.Map;

import eu.solven.adhoc.primitive.IValueProvider;

/**
 * An entry amongst the multiple entries of a {@link IDecomposition}.
 * 
 * Typically, for `slice=country:G8`, we would return 8 {@link IDecompositionEntry}, once for each country of the `G8`.
 * 
 * @author Benoit Lacelle
 */
public interface IDecompositionEntry {
	Map<String, ?> getSlice();

	IValueProvider getValue();

	static IDecompositionEntry of(Map<String, ?> slice, IValueProvider value) {
		return DecompositionEntry.builder().slice(slice).value(value).build();
	}

	static IDecompositionEntry of(Map<String, ?> slice, Object value) {
		if (value instanceof IValueProvider valueProvider) {
			return of(slice, valueProvider);
		} else {
			return of(slice, IValueProvider.setValue(value));
		}
	}
}
