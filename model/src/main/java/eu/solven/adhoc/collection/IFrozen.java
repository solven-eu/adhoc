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
package eu.solven.adhoc.collection;

/**
 * Marks a data structure that has gone through a one-way transition from mutable to read-only. After freezing, all
 * write operations must throw {@link UnsupportedOperationException}; reads remain valid.
 *
 * <p>
 * The transition is typically triggered by a finalisation step such as {@code compact()} or {@code closeColumn()}. Once
 * frozen, a structure cannot be unfrozen.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface IFrozen {

	/**
	 * Returns {@code true} if this object has been frozen and can no longer accept write operations.
	 *
	 * @return {@code true} once the one-way freeze transition has occurred
	 */
	boolean isFrozen();
}
