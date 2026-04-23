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
 * Pre-screen used by {@link MultitypeNavigableColumn#appendIfOptimal} to fast-reject keys that are definitely not
 * present in the column, avoiding the binary-search slow path.
 *
 * <p>
 * Implementations may be exact (e.g. {@link java.util.HashSet}-backed) or probabilistic (e.g. Guava
 * {@link com.google.common.hash.BloomFilter}-backed); the contract is the same:
 * <ul>
 * <li>{@link #mightContain(Object)} <strong>MUST</strong> return {@code true} for any key that has been passed to
 * {@link #add(Object)}.</li>
 * <li>{@link #mightContain(Object)} <em>MAY</em> return {@code true} for keys that were never added (false positives
 * are tolerated).</li>
 * <li>Implementations are <strong>not required</strong> to support removal — once a key is recorded, it stays
 * recorded.</li>
 * </ul>
 *
 * <p>
 * The minimal contract makes it cheap to swap implementations for benchmarking, testing (e.g.
 * {@link NoopKeyPresencePreScreen} replicates the legacy behavior of always falling through to the exact lookup), or
 * production tuning.
 *
 * @param <T>
 *            the key type
 * @author Benoit Lacelle
 */
public interface IKeyPresencePreScreen<T> {

	/**
	 * @param key
	 *            the key to test
	 * @return {@code false} if the key is <strong>definitely</strong> not present (the caller may skip the exact
	 *         lookup), {@code true} if the key <em>might</em> be present (the caller must perform the exact lookup to
	 *         confirm).
	 */
	boolean mightContain(T key);

	/**
	 * Records {@code key} as present. After this call, {@link #mightContain(Object)} for the same key must return
	 * {@code true}.
	 *
	 * @param key
	 *            the key to record
	 */
	void add(T key);
}
