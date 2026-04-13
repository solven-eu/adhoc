/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.encoding.page;

/**
 * Minimal {@code int -> Object} reader. Project-local equivalent of {@link java.util.function.IntFunction} without the
 * {@code java.util.function} dependency in hot paths, so that domain types can directly implement this interface and be
 * passed where an {@code int}-indexed reader is required — avoiding the bound-method-reference allocation (e.g.
 * {@code frozen::readValue}) at call sites that would otherwise adapt them to {@code IntFunction}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IInt2ObjectReader {

	/**
	 * @param index
	 *            the position to read
	 * @return the value at {@code index}
	 */
	Object read(int index);
}
