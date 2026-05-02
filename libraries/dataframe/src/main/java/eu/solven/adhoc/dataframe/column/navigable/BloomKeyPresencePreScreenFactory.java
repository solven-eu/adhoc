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
 * {@link IKeyPresencePreScreenFactory} producing {@link BloomKeyPresencePreScreen} instances sized from the column's
 * capacity, clamped between {@link #minExpected} and {@link #maxExpected}.
 *
 * <p>
 * Owns the tuning constants ({@code minExpected}, {@code maxExpected}, {@code fpp}) so they live alongside the
 * implementation that uses them rather than leaking into {@link MultitypeNavigableColumn}.
 *
 * @author Benoit Lacelle
 */
public class BloomKeyPresencePreScreenFactory implements IKeyPresencePreScreenFactory {

	/** Default expected-insertions floor: ensures the bloom is not undersized for tiny columns. */
	public static final int DEFAULT_MIN_EXPECTED = 64;

	/** Default expected-insertions ceiling: caps per-column memory at ~20 KB even when capacity is huge. */
	public static final int DEFAULT_MAX_EXPECTED = 1 << 14;

	/** Default false positive probability targeted by the bloom filter (1%). */
	public static final double DEFAULT_FPP = 0.01;

	/** Shared default-tuned instance — safe to reuse across columns and key types. */
	public static final BloomKeyPresencePreScreenFactory INSTANCE = new BloomKeyPresencePreScreenFactory();

	private final int minExpected;
	private final int maxExpected;
	private final double fpp;

	/**
	 * Creates a factory with the {@link #DEFAULT_MIN_EXPECTED}, {@link #DEFAULT_MAX_EXPECTED}, and {@link #DEFAULT_FPP}
	 * tuning.
	 */
	public BloomKeyPresencePreScreenFactory() {
		this(DEFAULT_MIN_EXPECTED, DEFAULT_MAX_EXPECTED, DEFAULT_FPP);
	}

	/**
	 * Creates a factory with explicit tuning.
	 *
	 * @param minExpected
	 *            expected-insertions floor (lower bound on the {@code expectedInsertions} passed to
	 *            {@link com.google.common.hash.BloomFilter#create})
	 * @param maxExpected
	 *            expected-insertions ceiling (upper bound)
	 * @param fpp
	 *            target false positive probability (e.g. {@code 0.01} for 1%)
	 */
	public BloomKeyPresencePreScreenFactory(int minExpected, int maxExpected, double fpp) {
		if (minExpected <= 0 || maxExpected < minExpected) {
			throw new IllegalArgumentException(
					"Invalid expected-insertions bounds: min=%d max=%d".formatted(minExpected, maxExpected));
		}
		if (fpp <= 0.0 || fpp >= 1.0) {
			throw new IllegalArgumentException("fpp must be in (0,1), got " + fpp);
		}
		this.minExpected = minExpected;
		this.maxExpected = maxExpected;
		this.fpp = fpp;
	}

	@Override
	public <T> IKeyPresencePreScreen<T> create(int capacity) {
		int expected = Math.clamp(capacity, minExpected, maxExpected);
		return new BloomKeyPresencePreScreen<>(expected, fpp);
	}
}
