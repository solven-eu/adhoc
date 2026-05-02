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
 * No-op {@link IKeyPresencePreScreen}: {@link #mightContain(Object)} always returns {@code true} and
 * {@link #add(Object)} is a no-op. Replicates the legacy behavior of {@link MultitypeNavigableColumn} where
 * {@code appendIfOptimal} always falls through to the exact {@code getIndex} binary search.
 *
 * <p>
 * Useful as:
 * <ul>
 * <li>a baseline in benchmarks (to measure the win provided by a real pre-screen),</li>
 * <li>a fallback for callers that disable the optimization,</li>
 * <li>a low-memory option when the binary-search slow path is rare enough that the bloom filter overhead is not
 * justified.</li>
 * </ul>
 *
 * @param <T>
 *            the key type
 * @author Benoit Lacelle
 */
public final class NoopKeyPresencePreScreen<T> implements IKeyPresencePreScreen<T> {
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private static final NoopKeyPresencePreScreen<Object> INSTANCE = new NoopKeyPresencePreScreen<>();

	/**
	 * @return the shared singleton instance, safely cast to the caller's key type.
	 */
	@SuppressWarnings("unchecked")
	public static <T> IKeyPresencePreScreen<T> instance() {
		return (IKeyPresencePreScreen<T>) INSTANCE;
	}

	private NoopKeyPresencePreScreen() {
		// Use NoopKeyPresencePreScreen#instance().
	}

	@Override
	public boolean mightContain(T key) {
		return true;
	}

	@Override
	public void add(T key) {
		// no-op
	}
}
