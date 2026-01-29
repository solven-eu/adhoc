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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import eu.solven.adhoc.fsst.v3.Counters.IntPair;
import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Constants for FSST training
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings("checkstyle:MagicNumber")
@Slf4j
@UtilityClass
public class FsstTrain {
	public static final boolean DEBUG = true;

	// --- Sampling constants ---
	// we construct FSST symbol tables using a random sample of about 16KB (1<<14)
	// https://github.com/cwida/fsst/blob/master/libfsst.hpp#L133
	public static final int FSST_SAMPLE_TARGET = 1 << 14; // 16KB
	public static final int FSST_SAMPLE_MAX_SZ = 2 * FSST_SAMPLE_TARGET;
	public static final int FSST_SAMPLE_LINE = 512;

	// --- Candidate weighting / gain ---
	public static final int SINGLE_BYTE_BOOST = 8;
	public static final int MIN_COUNT_NUMERATOR = 5;
	public static final int MIN_COUNT_DENOMINATOR = 128;
	public static final int RNG_SEED = 4_637_947;

	/**
	 * Train builds and finalizes a compression Table from the provided corpora. It samples inputs, iteratively parses
	 * and counts symbol usage, proposes merged symbols, retains top-gain candidates, and finalizes code layout.
	 */
	public static SymbolTable train(byte[][] inputs) {
		byte[][] sample = makeSample(inputs);
		SymbolTableTraining table = SymbolTableTraining.makeSymbolTable();

		Counters counter = new Counters();

		// long previousLengthEncoded = Long.MAX_VALUE;

		for (int frac = 8;; frac += 30) {
			// It is substantially faster (~ x2) to reset than creating a new instance
			// May become a bottleneck if trained+encoded on many small inputs
			if (frac > 8) {
				counter.reset();
			}

			// Simulate encode given known symbols, counting symbols use
			// Will read but not write symbolTable
			long lengthEncoded = compressCount(table, counter, sample, frac);

			assert lengthEncoded >= 0;

			if (DEBUG) {
				log.info("Length encoded: " + lengthEncoded);
			}

			// Build symbol tables given frequencies
			table = buildCandidates(table, counter, frac);
			if (frac >= 128) {
				// Authors recommend having 5 iterations
				break;
			}
		}
		return table.finalizeTable();
	}

	private record CodeAndLength(int code, int length) {

	}

	/**
	 * findNextSymbolFast returns the best match at data[position:] using the current Table: prefer 3–8 byte hash hits,
	 * then unique 2-byte short codes, otherwise fall back to single-byte. Returns code and matched length.
	 */
	public static CodeAndLength findNextSymbolFast(SymbolTableTraining t, byte[] data, int position) {
		long word = SymbolUtil.fsstUnalignedLoad(data, position);
		long prefix24 = word & IFsstConstants.fsstMask24;
		int hashIndex = (int) (SymbolUtil.fsstHash(prefix24) & (IFsstConstants.fsstHashTabSize - 1));
		Symbol hashSymbol = t.hashTab[hashIndex];
		int shortCode = t.shortCodes[(int) (word & IFsstConstants.fsstMask16)] & IFsstConstants.fsstCodeMask;
		long symbolMask = ~0L >>> hashSymbol.ignoredBits();
		long maskedWord = word & symbolMask;

		if (hashSymbol.icl < SymbolTableTraining.fsstICLFree && hashSymbol.val == maskedWord) {
			return new CodeAndLength(hashSymbol.code(), hashSymbol.length());
		}
		if (shortCode >= SymbolTableTraining.fsstCodeBase) {
			return new CodeAndLength(shortCode, 2);
		}
		return new CodeAndLength(t.byteCodes[(int) (word & IFsstConstants.fsstMask8)] & IFsstConstants.fsstCodeMask, 1);
	}

	// https://github.com/cwida/fsst/blob/master/libfsst.cpp#L54
	static boolean isEscapeCode(int pos) {
		return pos < IFsstConstants.fsstCodeBase;
	}

	static int codeLength(int pos) {
		if (isEscapeCode(pos)) {
			// If escaped, we write an escape byte, then the literal byte
			return 2;
		} else {
			// Else we just write the code as one byte
			return 1;
		}
	}

	/**
	 * compressCount walks the sample as the encoder would with the current Table, incrementing single counts and (in
	 * early rounds) pair counts to drive candidate selection in the subsequent build step.
	 * 
	 * See {@link SymbolTable#encode(byte[], byte[])}
	 */
	public static long compressCount(SymbolTableTraining t, Counters c, byte[][] samples, int frac) {
		// length of the encoded, restricted to sampled input
		int encodedLength = 0;

		for (int i = 0; i < samples.length; i++) {
			// in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
			// TODO Adjust with https://github.com/cwida/fsst/blob/master/libfsst.cpp#L89
			if (frac < 128 && (SymbolUtil.fsstHash(i) & IFsstConstants.fsstSampleMask) > frac) {
				continue;
			}

			byte[] sample = samples[i];
			final int end = sample.length;
			if (end == 0) {
				// empty input
				continue;
			}

			int cur = 0;

			// initialize code 1, as then we loop from consecutive code1->code2
			int code1 = t.findLongestSymbol(Symbol.fromBytes(sample, cur));
			{
				cur += t.symbols[code1].length();
				encodedLength += codeLength(code1);
			}

			int start = 0;

			while (true) {
				// count single symbol (i.e. an option is not extending it)
				c.incSingle(code1);

				// as an alternative, consider just using the next byte..
				if (t.symbols[code1].length() >= 2) {
					// .. but do not count single byte symbols doubly
					c.incSingle(sample[start] & 0xFF);
				}
				if (cur == end) {
					break;
				}

				// now match a new symbol
				start = cur;

				int code2;
				if (cur + 7 < end) {
					CodeAndLength result = findNextSymbolFast(t, sample, cur);
					code2 = result.code;
					cur += result.length;
				} else {
					code2 = t.findLongestSymbol(Symbol.fromBytes(sample, cur));
					cur += t.symbols[code2].length();
				}

				// compute compressed output size
				encodedLength += codeLength(code2);

				if (frac < 128) {
					int n = cur - start;
					c.incPair(code1, code2);
					if (n > 1) {
						c.incPair(code1, sample[start] & 0xFF);
					}
				}
				code1 = code2;
			}
		}

		// log.debug("Estimated gain is {}", gain);
		return encodedLength;
	}

	/**
	 * QSym represents a candidate symbol with gain.
	 */
	public record QSym(Symbol symbol, int gain) implements Comparable<QSym> {
		// larger val breaks tie
		public static final Comparator<QSym> COMPARATOR =
				Comparator.<QSym>comparingInt(q -> q.gain).thenComparingLong(q -> -q.symbol.val);

		@Override
		public int compareTo(QSym o) {
			return COMPARATOR.compare(this, o);
		}
	}

	/**
	 * buildCandidates creates symbol candidates from current counters. It boosts single bytes, considers merged pairs
	 * (except in the last round), scores by gain≈frequency×length, keeps top-K via a min-heap, and updates the Table.
	 * Reuses provided allocations to reduce GC pressure.
	 */
	@SuppressWarnings("PMD.AvoidReassigningLoopVariables")
	public static SymbolTableTraining buildCandidates(SymbolTableTraining t, Counters c, int frac) {
		Map<SymCodeless, QSym> candidates = HashMap.newHashMap(IFsstConstants.fsstCodeMax);

		// on first iteration, minCount is 1
		// on last iteration, minCount is 5
		int minCount = Math.max(MIN_COUNT_NUMERATOR * frac / MIN_COUNT_DENOMINATOR, 1);
		assert minCount > 0;

		// TODO Should improve this else we may discord good symbols on very short training data (e.g. just a
		// simple String)
		minCount = 0;

		// Iterate through code have appeared at least once
		for (int pos1 = 0; pos1 < IFsstConstants.fsstCodeBase + t.getNSymbols(); pos1++) {
			AtomicInteger codeHolder = new AtomicInteger(pos1);
			int count = c.nextNotZero(codeHolder); // may advance pos1!!;
			pos1 = codeHolder.get();

			if (count == 0) {
				// nextNotZero may not update current position, hence it may return a code with count=0
				continue;
			}

			// speed
			Symbol sym = t.symbols[pos1];
			int weight = count;
			if (sym.length() == 1) {
				// heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases
				// [de]compression
				weight *= SINGLE_BYTE_BOOST;
			}

			if (weight >= minCount) {
				addOrInc(candidates, sym, count);
			}
		}

		// If before last iteration , we try merging some symbols
		// Go implementation differs from C++ implementation by relying on a sparse list of pairs.
		if (frac < 128) {
			for (IntPair pair : c.getPairList()) {
				int code1 = pair.left();
				int code2 = pair.right();
				int countPair = c.pairCount(code1, code2);
				if (countPair < minCount) {
					continue;
				}
				Symbol sym = t.symbols[code1];
				Symbol sym2 = t.symbols[code2];
				if (sym.length() < 8) {
					// symbol has already max length: can not be suffixed
					Symbol merged = SymbolUtil.fsstConcat(sym, sym2);
					addOrInc(candidates, merged, countPair);
				}
				if (
				// symbol 2has already max length: can not be suffixed
				sym2.length() < 8
						// concatEnd would give different merge result than concat
						&& sym.length() + sym2.length() > 8) {
					Symbol merged = SymbolUtil.fsstConcatEnd(sym, sym2);
					addOrInc(candidates, merged, countPair);
				}
			}
		}

		List<QSym> sortedList;
		if (candidates.size() > SymbolUtil.fsstMaxSymbols) {
			// Extract top-K into list in descending order
			PriorityQueue<QSym> pq = new PriorityQueue<>(SymbolUtil.fsstMaxSymbols);

			for (QSym x : candidates.values()) {
				if (pq.size() < SymbolUtil.fsstMaxSymbols) {
					pq.offer(x);
				} else if (x.compareTo(pq.peek()) > 0) {
					// QSym polled =
					pq.poll();
					pq.offer(x);

					// System.out.println("Replaced " + polled + " with " + x);
				}
			}

			// Copy into least, from least to greater
			sortedList = new ArrayList<>(pq.size());
			while (!pq.isEmpty()) {
				sortedList.add(pq.poll());
			}
		} else {
			sortedList = new ArrayList<>(candidates.values());
			Collections.sort(sortedList);
		}

		SymbolTableTraining newSymboltable = SymbolTableTraining.makeSymbolTable();

		List<QSym> betterToWorst = sortedList.reversed();
		for (int i = 0; i < betterToWorst.size(); i++) {
			QSym q = betterToWorst.get(i);

			if (DEBUG) {
				if (newSymboltable.nSymbols == 0) {
					System.out.println("first symbol is " + q);
				} else if (i == sortedList.size() - 1) {
					System.out.println("last symbol is " + q);
				}
			}

			newSymboltable.addSymbol(q.symbol);
		}
		return newSymboltable;
	}

	/**
	 * 
	 * @param candidates
	 * @param sym
	 * @param weight
	 *            is typically count of occurrence
	 */
	private static void addOrInc(Map<SymCodeless, QSym> candidates, Symbol sym, int count) {
		SymCodeless key = new SymCodeless(sym.val, sym.length());

		// If coded, we write 1
		// if escaped, we write 2*length
		int gain = count * (2 * sym.length() - 1);

		if (candidates.containsKey(key)) {
			gain += candidates.get(key).gain;
		}
		candidates.put(key, new QSym(sym, gain));

	}

	/**
	 * Converts String to UTF-8 byte[] and trains a {@link SymbolTable}
	 */
	public static SymbolTable train(String... inputs) {
		byte[][] bytes = new byte[inputs.length][];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = inputs[i].getBytes(StandardCharsets.UTF_8);
		}
		return train(bytes);
	}

	/**
	 * Converts String to UTF-8 byte[] and trains a {@link SymbolTable}
	 */
	public static SymbolTable train(List<String> inputs) {
		byte[][] bytes = new byte[inputs.size()][];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = inputs.get(i).getBytes(StandardCharsets.UTF_8);
		}
		return train(bytes);
	}

	/**
	 * makeSample assembles a ~16KB deterministic pseudo-random sample composed of 512-byte slices from the inputs to
	 * keep training fast yet representative.
	 */
	public static byte[][] makeSample(byte[][] inputs) {
		int total = Arrays.stream(inputs).mapToInt(arr -> arr.length).sum();
		if (total < FSST_SAMPLE_TARGET) {
			// Input is small enough: no need to sample it
			return inputs;
		}

		byte[] buf = new byte[FSST_SAMPLE_MAX_SZ];
		List<byte[]> sample = new ArrayList<>(inputs.length);
		int pos = 0;
		long rng = SymbolUtil.fsstHash(RNG_SEED);

		while (pos < FSST_SAMPLE_MAX_SZ) {
			rng = SymbolUtil.fsstHash(rng);
			int idx = (int) Long.remainderUnsigned(rng, inputs.length);
			while (inputs[idx].length == 0) {
				idx = (idx + 1) % inputs.length;
			}

			int numChunks = (inputs[idx].length + FSST_SAMPLE_LINE - 1) / FSST_SAMPLE_LINE;
			rng = SymbolUtil.fsstHash(rng);
			int off = FSST_SAMPLE_LINE * (int) Long.remainderUnsigned(rng, numChunks);

			int n = Math.min(inputs[idx].length - off, FSST_SAMPLE_LINE);
			if (pos + n > FSST_SAMPLE_MAX_SZ) {
				break;
			}
			System.arraycopy(inputs[idx], off, buf, pos, n);
			sample.add(Arrays.copyOfRange(buf, pos, pos + n));
			pos += n;

			if (pos >= FSST_SAMPLE_TARGET) {
				break;
			}
		}
		return sample.toArray(new byte[0][]);
	}

	/**
	 * Utility class for keys in candidates map.
	 */
	public record SymCodeless(long val, int length) {
	}

}
