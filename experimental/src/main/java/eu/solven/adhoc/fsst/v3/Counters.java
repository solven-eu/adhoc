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
package eu.solven.adhoc.fsst.v3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;

/**
 * Used for training purposes, to count how a given {@link SymbolTable} would encounter given {@link Symbol} in some
 * input.
 * 
 * It also counts for consecutive symbols in order to enable symbol merging.
 * 
 * @author Benoit lacelle
 */
// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L340
// TODO There is an alternative implementation if not 32bits. Should we implement it?
@NotThreadSafe
public class Counters {
	// Must be relative to the maximum sampling size to prevent saturation
	private static final int MAX_COUNT = 0xFFFF;

	private static final int counterCodeMax = IFsstConstants.fsstCodeMax;

	// 1-byte may be counted by both literals (<256) and 1-byte symbols (>=256 but < 512)
	private final int[] single = new int[counterCodeMax]; // single-symbol counts
	// code1 * counterCodeMax + code2
	private final int[] pair = new int[counterCodeMax * counterCodeMax]; // pair counts

	// https://github.com/axiomhq/fsst/blob/main/counters.go
	private final List<IntPair> pairList = new ArrayList<>(); // sparse list of non-zero pairs

	// reset is faster than a new instance creation, as a training phase will typically needs 5 counters
	public void reset() {
		Arrays.fill(single, 0);
		Arrays.fill(pair, 0);
		pairList.clear();
	}

	record IntPair(int left, int right) {
	}

	/**
	 * Increment the frequency count for a single symbol (capped at 0xFFFF).
	 * 
	 * If code < 256, we refer to a literal byte, else (but still <512) we refer to a symbol (which may be 1-byte)
	 */
	public void incSingle(int code) {
		assert code >= 0 && code <= counterCodeMax : "Invalid code: %s".formatted(code);

		if (single[code] < MAX_COUNT) {
			single[code]++;
		}
	}

	/** Increment the frequency count for a symbol pair (sparse tracking). */
	public void incPair(int code1, int code2) {
		int combinedCode = code1 * counterCodeMax + code2;
		if (pair[combinedCode] == 0) {
			pairList.add(new IntPair(code1, code2));
			pair[combinedCode] = 1;
		} else if (pair[combinedCode] < MAX_COUNT) {
			pair[combinedCode]++;
		}
	}

	/**
	 * Advances code to the next non-zero count and returns it. codeHolder[0] acts like Go's pointer to uint32. Returns
	 * 0 if no more non-zero counts exist.
	 */
	// BEWARE This seems to refer to count1GetNext, but it is much different from C++ version
	// https://github.com/axiomhq/fsst/blob/main/counters.go#L36
	// https://github.com/cwida/fsst/blob/master/libfsst.cpp#L183
	public int nextNotZero(AtomicInteger codeHolder) {
		int code = codeHolder.get();
		while (code < IFsstConstants.fsstCodeMax) {
			int count = single[code];
			if (count != 0) {
				codeHolder.set(code);
				return count;
			}
			code++;
		}
		codeHolder.set(code);
		return 0;
	}

	/**
	 * 
	 * @param code1
	 * @param codeHolder
	 *            hold initial code2, but may change it to a greater code with a non-zero count
	 * @return 0 if there is no more non-zero codes
	 */
	public int nextNotZero(int code1, AtomicInteger codeHolder) {
		int code2 = codeHolder.get();
		while (code2 < IFsstConstants.fsstCodeMax) {
			int combinedCode = code1 * counterCodeMax + code2;
			int count = pair[combinedCode];
			if (count != 0) {
				codeHolder.set(code2);
				return count;
			}
			code2++;
		}
		codeHolder.set(code2);
		return 0;
	}

	public int singleCount(int code) {
		return single[code];
	}

	/** Returns the count for a specific symbol pair. */
	public int pairCount(int code1, int code2) {
		int combinedCode = code1 * counterCodeMax + code2;
		return pair[combinedCode];
	}

	public List<IntPair> getPairList() {
		return Collections.unmodifiableList(pairList);
	}
}