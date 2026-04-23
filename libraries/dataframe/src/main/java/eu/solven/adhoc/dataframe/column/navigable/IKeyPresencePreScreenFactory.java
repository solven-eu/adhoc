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
package eu.solven.adhoc.dataframe.column.navigable;

/**
 * Factory for {@link IKeyPresencePreScreen} instances. A single factory can be reused across many
 * {@link MultitypeNavigableColumn} instances and many key types — implementations are responsible for sizing the
 * pre-screen based on the column's expected capacity.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IKeyPresencePreScreenFactory {

	/**
	 * Creates a fresh pre-screen typed for the caller's key type {@code T}.
	 *
	 * @param capacity
	 *            the column's expected number of distinct keys; the factory uses this as a sizing hint
	 * @param <T>
	 *            the key type the pre-screen will be parameterised on
	 * @return a fresh, empty {@link IKeyPresencePreScreen}
	 */
	<T> IKeyPresencePreScreen<T> create(int capacity);
}
