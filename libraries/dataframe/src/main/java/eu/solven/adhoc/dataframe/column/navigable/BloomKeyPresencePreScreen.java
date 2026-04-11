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

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * {@link IKeyPresencePreScreen} backed by a Guava {@link BloomFilter}. Probabilistic: a positive
 * {@link #mightContain(Object)} result may be a false positive whose rate is bounded by the {@code fpp} parameter.
 *
 * <p>
 * <strong>BEWARE</strong> Bloom filters by design do not support removal. Once a key has been {@link #add(Object)},
 * {@link #mightContain(Object)} will report it as potentially present forever, even if the caller later considers the
 * key removed (e.g. {@code MultitypeNavigableColumn} purges a null write via {@code lazyClearLastWrite}). This is
 * harmless when the pre-screen is used as a fast-reject filter in front of an exact lookup: a false positive just falls
 * through to the exact path, never the other way around.
 *
 * @param <T>
 *            the key type
 * @author Benoit Lacelle
 */
public class BloomKeyPresencePreScreen<T> implements IKeyPresencePreScreen<T> {

	/**
	 * Default {@link Funnel} usable for any {@code T}: serializes the key via {@link String#valueOf(Object)}. Costs one
	 * {@code toString} allocation per put/contains; override the constructor argument with a typed funnel for higher
	 * throughput when {@code T} is known.
	 *
	 * <p>
	 * Implemented as an enum singleton so it is naturally {@link java.io.Serializable} (Guava
	 * {@link BloomFilter#create} requires a {@link java.io.Serializable} funnel).
	 */
	public enum DefaultFunnel implements Funnel<Object> {
		INSTANCE;

		@Override
		public void funnel(Object from, PrimitiveSink into) {
			into.putUnencodedChars(String.valueOf(from));
		}
	}

	private final BloomFilter<T> bloom;

	/**
	 * Creates a Bloom-backed pre-screen.
	 *
	 * @param funnel
	 *            funnel used to feed keys into the bloom filter; must be {@link java.io.Serializable}
	 * @param expectedInsertions
	 *            expected number of insertions; sizes the bloom filter
	 * @param fpp
	 *            target false positive probability (e.g. {@code 0.01} for 1%)
	 */
	public BloomKeyPresencePreScreen(Funnel<? super T> funnel, int expectedInsertions, double fpp) {
		this.bloom = BloomFilter.create(funnel, expectedInsertions, fpp);
	}

	/**
	 * Convenience constructor using {@link DefaultFunnel#INSTANCE}.
	 *
	 * @param expectedInsertions
	 *            expected number of insertions
	 * @param fpp
	 *            target false positive probability
	 */
	public BloomKeyPresencePreScreen(int expectedInsertions, double fpp) {
		this(DefaultFunnel.INSTANCE, expectedInsertions, fpp);
	}

	@Override
	public boolean mightContain(T key) {
		return bloom.mightContain(key);
	}

	@Override
	public void add(T key) {
		bloom.put(key);
	}
}
