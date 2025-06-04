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
package eu.solven.adhoc.exception;

import java.util.concurrent.CompletionException;

import lombok.experimental.UtilityClass;

/**
 * Utilities related to {@link Exception} and {@link Throwable}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocExceptionHelpers {

	public static RuntimeException wrap(RuntimeException e, String eMsg) {
		if (e instanceof IllegalStateException illegalStateE) {
			// We want to keep bubbling an IllegalStateException
			return new IllegalStateException(eMsg, illegalStateE);
		} else if (e instanceof CompletionException completionE) {
			if (completionE.getCause() instanceof IllegalStateException) {
				// We want to keep bubbling an IllegalStateException
				return new IllegalStateException(eMsg, completionE);
			} else {
				return new IllegalArgumentException(eMsg, completionE);
			}
		} else {
			return new IllegalArgumentException(eMsg, e);
		}
	}

}
