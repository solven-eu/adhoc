package eu.solven.adhoc.fsst.v3;

import java.util.ArrayList;
import java.util.List;

// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L340
// TODO There is an alternative implementation if not 32bits. Should we implement it?
public class Counters {

	// --- Fields ---
	private final int[] single = new int[IFsstConstants.fsstCodeMax]; // single-symbol counts
	private final int[][] pair = new int[IFsstConstants.fsstCodeMax][IFsstConstants.fsstCodeMax]; // pair counts

	public record IntPair(int left, int right) {
	}

	// https://github.com/axiomhq/fsst/blob/main/counters.go
	private final List<IntPair> pairList = new ArrayList<>(); // sparse list of non-zero pairs

	// --- Methods ---

	/** Increment the frequency count for a single symbol (capped at 0xFFFF). */
	public void incSingle(int code) {
		assert code >= 0 && code <= IFsstConstants.fsstCodeMax : "Invalid code: %s".formatted(code);

		if (single[code] < 0xFFFF) {
			single[code]++;
		}
	}

	/** Increment the frequency count for a symbol pair (sparse tracking). */
	public void incPair(int code1, int code2) {
		if (pair[code1][code2] == 0) {
			pairList.add(new IntPair(code1, code2));
		}
		if (pair[code1][code2] < 0xFFFF) {
			pair[code1][code2]++;
		}
	}

	/**
	 * Advances code to the next non-zero count and returns it. codeHolder[0] acts like Go's pointer to uint32. Returns
	 * 0 if no more non-zero counts exist.
	 */
	// BEWARE This seems to refer to count1GetNext, but it is much different from C++ version
	// https://github.com/axiomhq/fsst/blob/main/counters.go#L36
	// https://github.com/cwida/fsst/blob/master/libfsst.cpp#L183
	public int nextNotZero(int[] codeHolder) {
		int code = codeHolder[0];
		while (code < IFsstConstants.fsstCodeMax) {
			int count = single[code];
			if (count != 0) {
				codeHolder[0] = code;
				return count;
			}
			code++;
		}
		codeHolder[0] = code; // update caller's reference
		return 0;
	}

	/** Returns the count for a specific symbol pair. */
	public int pairCount(int code1, int code2) {
		return pair[code1][code2];
	}

	// --- Accessors for testing / iteration ---
	public int[] getSingle() {
		return single;
	}

	public int[][] getPair() {
		return pair;
	}

	public List<IntPair> getPairList() {
		return pairList;
	}
}