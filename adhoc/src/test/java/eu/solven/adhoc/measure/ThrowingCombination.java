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
package eu.solven.adhoc.measure;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;

/**
 * A {@link ICombination} which throws. Useful to check behaviors on exceptions.
 */
public class ThrowingCombination implements ICombination {
	/**
	 * A custom {@link RuntimeException}, enabling finer unit-tests.
	 * 
	 * @author Benoit Lacelle
	 */
	public static class ThrowingCombinationException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ThrowingCombinationException(String message, Throwable cause) {
			super(message, cause);
		}

		public ThrowingCombinationException(String message) {
			super(message);
		}
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		throw makeException(slice.getSlice().getCoordinates());
	}

	public static ThrowingCombinationException makeException(Map<String, ?> sliceAsMap) {
		return new ThrowingCombinationException("Throwing on slice=%s".formatted(sliceAsMap));
	}
}
