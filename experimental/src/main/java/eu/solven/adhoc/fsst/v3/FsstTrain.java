package eu.solven.adhoc.fsst.v3;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import eu.solven.adhoc.fsst.v3.Counters.IntPair;
import eu.solven.adhoc.fsst.v3.SymbolUtil.Symbol;

/**
 * Constants for FSST training
 */
public class FsstTrain {

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
	public static final int RNG_SEED = 4637947;

	/**
	 * Train builds and finalizes a compression Table from the provided corpora. It samples inputs, iteratively parses
	 * and counts symbol usage, proposes merged symbols, retains top-gain candidates, and finalizes code layout.
	 */
	public static SymbolTable2 train(byte[][] inputs) {
		byte[][] sample = makeSample(inputs);
		SymbolTable table = SymbolTable.makeSymbolTable();
		Counters counter = new Counters();

		for (int frac = 8;; frac += 30) {
			counter = new Counters();
			// Simulate encode given known symbols, counting symbols use
			// Will read but not write symbolTable
			compressCount(table, counter, sample, frac);
			// Build symbol tables given frequencies
			table = buildCandidates(table, counter, frac);
			if (frac >= 128) {
				// Authors recommend having 5 iterations
				break;
			}
		}
		return table.finalizeTable();
	}

	private static record CodeAndLength(int code, int length) {

	}

	/**
	 * findNextSymbolFast returns the best match at data[position:] using the current Table: prefer 3–8 byte hash hits,
	 * then unique 2-byte short codes, otherwise fall back to single-byte. Returns code and matched length.
	 */
	public static CodeAndLength findNextSymbolFast(SymbolTable t, byte[] data, int position) {
		long word = SymbolUtil.fsstUnalignedLoad(data, position);
		long prefix24 = word & IFsstConstants.fsstMask24;
		int hashIndex = (int) (SymbolUtil.fsstHash(prefix24) & (IFsstConstants.fsstHashTabSize - 1));
		Symbol hashSymbol = t.hashTab[hashIndex];
		int shortCode = t.shortCodes[(int) (word & IFsstConstants.fsstMask16)] & IFsstConstants.fsstCodeMask;
		long symbolMask = ~0L >>> hashSymbol.ignoredBits();
		long maskedWord = word & symbolMask;

		if (hashSymbol.icl < SymbolTable.fsstICLFree && hashSymbol.val == maskedWord) {
			return new CodeAndLength(hashSymbol.code(), (int) hashSymbol.length());
		}
		if (shortCode >= SymbolTable.fsstCodeBase) {
			return new CodeAndLength(shortCode, 2);
		}
		return new CodeAndLength(t.byteCodes[(int) (word & IFsstConstants.fsstMask8)] & IFsstConstants.fsstCodeMask, 1);
	}

	// https://github.com/cwida/fsst/blob/master/libfsst.cpp#L54
	static boolean isEscapeCode(int pos) {
		return pos < IFsstConstants.fsstCodeBase;
	}

	/**
	 * compressCount walks the sample as the encoder would with the current Table, incrementing single counts and (in
	 * early rounds) pair counts to drive candidate selection in the subsequent build step.
	 * 
	 * See {@link SymbolTable#encode(byte[], byte[])}
	 */
	public static void compressCount(SymbolTable t, Counters c, byte[][] samples, int frac) {
		int gain = 0;

		for (int i = 0; i < samples.length; i++) {
			// in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
			// TODO Adjust with https://github.com/cwida/fsst/blob/master/libfsst.cpp#L89
			if (frac < 128 && (SymbolUtil.fsstHash(i) & IFsstConstants.fsstSampleMask) > frac)
				continue;

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
				gain += t.symbols[code1].length() - (1 + (isEscapeCode(code1) ? 1 : 0));
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
				gain += (cur - start) - (1 + (isEscapeCode(code2) ? 1 : 0));

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
	}

	/**
	 * QSym represents a candidate symbol with gain.
	 */
	public static record QSym(Symbol symbol, int gain) {
	}

	/**
	 * QSymHeap is a min-heap of QSym based on gain (with tiebreak on symbol.val).
	 */
	public static class QSymHeap extends PriorityQueue<QSym> {
		private static final long serialVersionUID = 3582364282793277022L;

		// larger val breaks tie
		public static final Comparator<QSym> comparator =
				Comparator.<QSym>comparingInt(q -> q.gain).thenComparingLong(q -> -q.symbol.val);

		public QSymHeap() {
			super(
					// +1 as we add before removing smallest
					SymbolUtil.fsstMaxSymbols + 1,
					comparator);
		}
	}

	/**
	 * buildCandidates creates symbol candidates from current counters. It boosts single bytes, considers merged pairs
	 * (except in the last round), scores by gain≈frequency×length, keeps top-K via a min-heap, and updates the Table.
	 * Reuses provided allocations to reduce GC pressure.
	 */
	public static SymbolTable buildCandidates(SymbolTable t, Counters c, int frac) {
		Map<SymCodeless, QSym> candidates = HashMap.newHashMap(IFsstConstants.fsstCodeMax);
		int minCount = Math.max((MIN_COUNT_NUMERATOR * frac) / MIN_COUNT_DENOMINATOR, 1);
		assert minCount > 0;

		// Iterate through code have appeared at least once
		for (int pos1 = 0; pos1 < IFsstConstants.fsstCodeBase + t.getNSymbols(); pos1++) {
			int[] codeHolder = new int[] { pos1 };
			int count = c.nextNotZero(codeHolder); // may advance pos1!!;
			pos1 = codeHolder[0];

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
				addOrInc(candidates, sym, weight);
			}
		}

		// If before last iteration , we try merging some symbols
		// Go implementation differs from C++ implementation by relying on a sparse list of pairs.
		if (frac < 128) {
			for (IntPair pair : c.getPairList()) {
				int code1 = pair.left(), code2 = pair.right();
				int countPair = c.pairCount(code1, code2);
				if (countPair < minCount)
					continue;
				Symbol sym = t.symbols[code1];
				if (sym.length() == 8) {
					// symbol has already max length: can not be suffixed
					continue;
				}
				Symbol sym2 = t.symbols[code2];
				Symbol merged = SymbolUtil.fsstConcat(sym, sym2);

				addOrInc(candidates, merged, countPair);
			}
		}

		// Populate heap
		QSymHeap heap = new QSymHeap();

		for (QSym candidate : candidates.values()) {
			heap.add(candidate);

			if (heap.size() > SymbolUtil.fsstMaxSymbols) {
				// Remove smallest
				// QSym polled =
				heap.poll();
				// System.out.println("removed: " + polled);
			}
		}

		// Extract top-K into list in descending order
		List<QSym> sortedList =
				candidates.values().stream().sorted(QSymHeap.comparator).limit(SymbolUtil.fsstMaxSymbols).toList();

		SymbolTable newSymboltable = SymbolTable.makeSymbolTable();
		for (QSym q : sortedList.reversed()) {
			if (!newSymboltable.addSymbol(q.symbol)) {
				break;
			}
		}
		return newSymboltable;
	}

	private static void addOrInc(Map<SymCodeless, QSym> candidates, Symbol sym, int weight) {
		SymCodeless key = new SymCodeless(sym.val, sym.length());

		int gain = weight * sym.length();

		if (candidates.containsKey(key)) {
			gain += candidates.get(key).gain;
		}
		candidates.put(key, new QSym(sym, gain));

	}

	/**
	 * TrainStrings converts String[] to byte[][] and calls Train.
	 */
	public static SymbolTable2 train(String[] inputs) {
		byte[][] bytes = new byte[inputs.length][];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = inputs[i].getBytes(StandardCharsets.UTF_8);
		}
		return train(bytes);
	}

	public static SymbolTable2 train(List<String> inputs) {
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
		if (total < FSST_SAMPLE_TARGET)
			return inputs;

		byte[] buf = new byte[FSST_SAMPLE_MAX_SZ];
		List<byte[]> sample = new ArrayList<>(inputs.length);
		int pos = 0;
		long rng = SymbolUtil.fsstHash(RNG_SEED);

		while (pos < FSST_SAMPLE_MAX_SZ) {
			rng = SymbolUtil.fsstHash(rng);
			int idx = (int) (rng % inputs.length);
			while (inputs[idx].length == 0)
				idx = (idx + 1) % inputs.length;

			int numChunks = (inputs[idx].length + FSST_SAMPLE_LINE - 1) / FSST_SAMPLE_LINE;
			rng = SymbolUtil.fsstHash(rng);
			int off = FSST_SAMPLE_LINE * (int) (rng % numChunks);

			int n = Math.min(inputs[idx].length - off, FSST_SAMPLE_LINE);
			if (pos + n > FSST_SAMPLE_MAX_SZ)
				break;
			System.arraycopy(inputs[idx], off, buf, pos, n);
			sample.add(Arrays.copyOfRange(buf, pos, pos + n));
			pos += n;

			if (pos >= FSST_SAMPLE_TARGET)
				break;
		}
		return sample.toArray(new byte[0][]);
	}

	/**
	 * Utility class for keys in candidates map.
	 */
	public static record SymCodeless(long val, int length) {
	}

	/**
	 * Simple Pair class for returning (code, advance) tuples.
	 */
	public static class Pair<F, S> {
		public final F first;
		public final S second;

		public Pair(F first, S second) {
			this.first = first;
			this.second = second;
		}
	}

}
