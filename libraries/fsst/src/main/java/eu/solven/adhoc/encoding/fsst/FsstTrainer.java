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
package eu.solven.adhoc.encoding.fsst;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.errorprone.annotations.ThreadSafe;

import eu.solven.adhoc.encoding.bytes.IByteSlice;
import eu.solven.adhoc.encoding.fsst.SymbolUtil.Symbol;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Constants for FSST training
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.GodClass" })
@Slf4j
@Builder
@ThreadSafe
public class FsstTrainer {

	@Default
	@NonNull
	final FsstTrainerConfig config = FsstTrainerConfig.builder().build();

	public IFsstEncoding train(byte[] inputs) {
		return train(new byte[][] { inputs });
	}

	/**
	 * Train builds and finalizes a compression Table from the provided corpora. It samples inputs, iteratively parses
	 * and counts symbol usage, proposes merged symbols, retains top-gain candidates, and finalizes code layout.
	 */
	public IFsstEncoding train(byte[][] inputs) {
		SamplingResult sample = makeSample(config, Stream.of(inputs).map(IByteSlice::wrap).toList());
		return train(sample);
	}

	public IFsstEncoding train(List<IByteSlice> inputs) {
		SamplingResult sample = makeSample(config, inputs);
		return train(sample);
	}

	/**
	 * Holds the idea that only 2 Counters are needed for a training, one of the them being retained in the `best`
	 * symbol table while the other is available for process.
	 */
	private static final class CountersPair {
		// Pre-allocate two Counters up-front. When one is saved as bestCounters it must
		// not be overwritten, so we alternate to the other slot instead of allocating.
		final Counters first = new Counters();
		final Counters second = new Counters();

		boolean firstIsFreeElseSecond = true;

		boolean firstIsCleared = true;
		boolean secondIsCleared = true;

		private Counters pickFree() {
			// If counter was promoted to bestCounters, switch to the other pre-allocated slot.
			firstIsFreeElseSecond = !firstIsFreeElseSecond;

			if (firstIsFreeElseSecond) {
				if (secondIsCleared) {
					secondIsCleared = false;
				} else {
					second.reset();
				}

				return second;
			} else {
				if (firstIsCleared) {
					firstIsCleared = false;
				} else {
					first.reset();
				}

				return first;
			}
		}

		private void canReuse() {
			// previous `.pickFree()` toggled this: we restore the fact we should keep using the same Counters.
			firstIsFreeElseSecond = !firstIsFreeElseSecond;
		}
	}

	protected SymbolTable train(SamplingResult sample) {
		SymbolTableTraining table = SymbolTableTraining.makeSymbolTable();

		CountersPair countersPair = new CountersPair();

		long bestLengthEncoded = Long.MAX_VALUE;
		Counters bestCounters = null;
		SymbolTableTraining bestTable = null;

		int maxFrac = 128;

		for (int frac = 8;; frac += 30) {
			Counters counter = countersPair.pickFree();

			// Simulate encode given known symbols, counting symbols use
			// Will read but not write symbolTable
			long lengthEncoded = compressCount(table, counter, sample, frac);

			assert lengthEncoded >= 0;

			if (lengthEncoded < bestLengthEncoded) {
				bestLengthEncoded = lengthEncoded;
				bestCounters = counter;
				bestTable = table;
			} else {
				countersPair.canReuse();
			}

			if (frac >= maxFrac) {
				// Authors recommend having 5 iterations
				break;
			} else {
				// Build symbol tables given frequencies
				table = buildCandidates(table, counter, frac, sample.isSampled);
			}
		}

		bestTable = buildCandidates(bestTable, bestCounters, maxFrac, sample.isSampled);

		// renumber codes for more efficient compression
		return bestTable.finalizeTable();
	}

	private record CodeAndLength(int code, int length) {

	}

	CodeAndLength findNextSymbolFast(SymbolTableTraining t, long word) {
		long prefix24 = word & IFsstConstants.fsstMask24;
		int hashIndex = (int) (SymbolUtil.fsstHash(prefix24) & (IFsstConstants.fsstHashTabSize - 1));
		Symbol hashSymbol = t.hashTab[hashIndex];
		// TODO Should next row be pushed downward?
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

	/**
	 * findNextSymbolFast returns the best match at data[position:] using the current Table: prefer 3–8 byte hash hits,
	 * then unique 2-byte short codes, otherwise fall back to single-byte. Returns code and matched length.
	 */
	public CodeAndLength findNextSymbolFast(SymbolTableTraining t, byte[] data, int position) {
		long word = SymbolUtil.fsstUnalignedLoad(data, position);
		return findNextSymbolFast(t, word);
	}

	/**
	 * findNextSymbolFast returns the best match at data[position:] using the current Table: prefer 3–8 byte hash hits,
	 * then unique 2-byte short codes, otherwise fall back to single-byte. Returns code and matched length.
	 */
	public CodeAndLength findNextSymbolFast(SymbolTableTraining t, IByteSlice data, int position) {
		long word = SymbolUtil.fsstUnalignedLoad(data, position);
		return findNextSymbolFast(t, word);
	}

	// https://github.com/cwida/fsst/blob/master/libfsst.cpp#L54
	boolean isEscapeCode(int pos) {
		return pos < IFsstConstants.fsstCodeBase;
	}

	int encodedLength(int pos) {
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
	protected long compressCount(SymbolTableTraining t, Counters c, SamplingResult samplingResult, int frac) {
		// length of the encoded, restricted to sampled input
		int encodedLength = 0;

		// Register code actually used to estimate the encoded length on a table pruned of unused symbols
		boolean[] codeUsed = new boolean[2 * IFsstConstants.fsstCodeBase];

		for (IByteSlice sample : samplingResult.samples) {
			if (sample == null) {
				continue;
			}

			// in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
			// TODO Adjust with https://github.com/cwida/fsst/blob/master/libfsst.cpp#L89
			// TODO To be adjusted based on sample size, else we might fully skip some round if the training input is
			// very small
			// BEWARE It would also bias the output if it count absolute gain. Should we count relative gain? (gain /
			// sampled input)
			// if (frac < 128 && (SymbolUtil.fsstHash(i) & IFsstConstants.fsstSampleMask) > frac) {
			// continue;
			// }
			if (sample.isFastCrop()) {
				// When possible (e.g. on small string), we rely on a byte[] which is faster than IByteSlice abstraction
				byte[] sampleAsArray = sample.crop();

				final int end = sampleAsArray.length;
				if (end == 0) {
					// empty input
					continue;
				}

				encodedLength = compressCount(t, c, frac, encodedLength, codeUsed, sample, sampleAsArray, end);
			} else {
				final int end = sample.length();
				if (end == 0) {
					// empty input
					continue;
				}

				encodedLength = compressCount(t, c, frac, encodedLength, codeUsed, sample, end);
			}
		}

		// account for the encoded symbol table size
		for (int i = 0; i < t.nSymbols; i++) {
			if (codeUsed[IFsstConstants.fsstCodeBase + i]) {
				// account only for symbols in table actually used in the compress pass
				int symLength = t.symbols[IFsstConstants.fsstCodeBase + i].length();
				encodedLength += symLength;
				// account for the array of length
				encodedLength++;
			}
		}

		// log.debug("Estimated gain is {}", gain);
		return encodedLength;
	}

	private int compressCount(SymbolTableTraining t,
			Counters c,
			int frac,
			int encodedLength,
			boolean[] codeUsed,
			IByteSlice sample,
			final int end) {
		int cur = 0;

		// initialize code 1, as then we loop from consecutive code1->code2
		int code1 = t.findLongestSymbol(Symbol.fromBytes(sample, cur));
		{
			cur += t.symbols[code1].length();
			encodedLength += encodedLength(code1);
			codeUsed[code1] = true;
		}

		int start = 0;

		while (true) {
			// count single symbol (i.e. an option is not extending it)
			c.incSingle(code1);

			// as an alternative, consider just using the next byte
			if (frac < 128 && cur > start + 1) {
				// Prevent double-counting by skipping single-byte symbols
				c.incSingle(sample.read(start) & 0xFF);
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
			encodedLength += encodedLength(code2);
			codeUsed[code2] = true;

			if (frac < 128) {
				// No need to count pairs in last pass
				c.incPair(code1, code2);
				if (t.symbols[code2].length() > 1) {
					// Prevent double-counting by skipping single-byte symbols
					c.incPair(code1, sample.read(start) & 0xFF);
				}
			}
			code1 = code2;
		}
		return encodedLength;
	}

	private int compressCount(SymbolTableTraining t,
			Counters c,
			int frac,
			int encodedLength,
			boolean[] codeUsed,
			IByteSlice sample,
			byte[] sampleAsArray,
			final int end) {
		int cur = 0;

		// initialize code 1, as then we loop from consecutive code1->code2
		int code1 = t.findLongestSymbol(Symbol.fromBytes(sampleAsArray, cur));
		{
			cur += t.symbols[code1].length();
			encodedLength += encodedLength(code1);
			codeUsed[code1] = true;
		}

		int start = 0;

		while (true) {
			// count single symbol (i.e. an option is not extending it)
			c.incSingle(code1);

			// as an alternative, consider just using the next byte
			if (frac < 128 && cur > start + 1) {
				// Prevent double-counting by skipping single-byte symbols
				c.incSingle(sampleAsArray[start] & 0xFF);
			}
			if (cur == end) {
				break;
			}

			// now match a new symbol
			start = cur;

			int code2;
			if (cur + 7 < end) {
				CodeAndLength result = findNextSymbolFast(t, sampleAsArray, cur);
				code2 = result.code;
				cur += result.length;
			} else {
				code2 = t.findLongestSymbol(Symbol.fromBytes(sampleAsArray, cur));
				cur += t.symbols[code2].length();
			}

			// compute compressed output size
			encodedLength += encodedLength(code2);
			codeUsed[code2] = true;

			if (frac < 128) {
				// No need to count pairs in last pass
				c.incPair(code1, code2);
				if (t.symbols[code2].length() > 1) {
					// Prevent double-counting by skipping single-byte symbols
					c.incPair(code1, sample.read(start) & 0xFF);
				}
			}
			code1 = code2;
		}
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
	protected SymbolTableTraining buildCandidates(SymbolTableTraining t, Counters c, int frac, boolean sampled) {
		int minCount = minCount(frac, sampled);

		Map<SymCodeless, QSym> candidates = buildCandidates(t, c, frac, minCount);

		List<QSym> sortedList = sortCandidates(candidates);

		SymbolTableTraining newSymbolTable = SymbolTableTraining.makeSymbolTable();

		return finalizeCandidates(frac, sortedList, newSymbolTable);
	}

	private SymbolTableTraining finalizeCandidates(int frac,
			List<QSym> sortedList,
			SymbolTableTraining newSymboltable) {
		List<QSym> betterToWorst = sortedList.reversed();
		for (QSym q : betterToWorst) {
			if (frac >= 128) {
				// last pass: keep symbol if effective
				// cost is symbol length + length reference
				int gain = q.gain;

				if (q.symbol.length() == 1) {
					gain /= config.getSingleByteBoost();
				}

				int cost = q.symbol.length() + 1;

				if (gain > cost) {
					newSymboltable.addSymbol(q.symbol);
				}
			} else {
				// intermediate pass: keep as many symbols as possible
				newSymboltable.addSymbol(q.symbol);
			}
		}
		return newSymboltable;
	}

	private List<QSym> sortCandidates(Map<SymCodeless, QSym> candidates) {
		List<QSym> sortedList;
		if (candidates.size() > SymbolUtil.fsstMaxSymbols) {
			// Extract top-K into list in descending order
			Queue<QSym> pq = new PriorityQueue<>(SymbolUtil.fsstMaxSymbols);

			for (QSym x : candidates.values()) {
				if (pq.size() < SymbolUtil.fsstMaxSymbols) {
					pq.offer(x);
				} else if (x.compareTo(pq.peek()) > 0) {
					// QSym polled =
					pq.poll();
					pq.offer(x);
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
		return sortedList;
	}

	/**
	 * 
	 * @param t
	 * @param c
	 * @param frac
	 *            from 0 to 128, growing on each pass. 128 means this is the final pass.
	 * @param minCount
	 * @return
	 */
	@SuppressWarnings("PMD.AvoidReassigningLoopVariables")
	private Map<SymCodeless, QSym> buildCandidates(SymbolTableTraining t, Counters c, int frac, int minCount) {
		Map<SymCodeless, QSym> candidates = HashMap.newHashMap(IFsstConstants.fsstCodeMax);
		// Iterate through symbols have appeared at least once
		int maxCodeExcluded = IFsstConstants.fsstCodeBase + t.getNSymbols();
		nextCode1: for (int pos1 = 0; pos1 < maxCodeExcluded; pos1++) {
			AtomicInteger codeHolder = new AtomicInteger(pos1);
			int count = c.nextNotZero(codeHolder); // may advance pos1!!;
			pos1 = codeHolder.get();

			if (count == 0) {
				// nextNotZero may not update current position, hence it may return a code with count=0
				continue nextCode1;
			}

			Symbol sym1 = t.symbols[pos1];
			{
				// speed
				int weight = count;
				if (sym1.length() == 1) {
					// heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases
					// [de]compression
					// TODO Should not be applied on latest step
					weight *= config.getSingleByteBoost();
				}

				if (weight >= minCount) {
					addOrInc(candidates, sym1, weight);
				}
			}

			// don't need pair-wise counts for last pass to just encode the data
			if (frac < 128 && sym1.length() < 8) {
				// symbol has already max length: can not be suffixed

				for (int pos2 = 0; pos2 < maxCodeExcluded; pos2++) {
					codeHolder.set(pos2);
					int countPair = c.nextNotZero(pos1, codeHolder);// may advance pos2!!;

					if (countPair == 0) {
						continue nextCode1;
					}
					pos2 = codeHolder.get();

					if (countPair >= minCount) {
						Symbol sym2 = t.symbols[pos2];

						Symbol merged = SymbolUtil.fsstConcat(sym1, sym2);
						addOrInc(candidates, merged, countPair);
					}
				}
			}
		}

		// // If before last iteration , we try merging some symbols
		// // Go implementation differs from C++ implementation by relying on a sparse list of pairs.
		// if (frac < 128) {
		// for (IntPair pair : c.getPairList()) {
		// int code1 = pair.left();
		// int code2 = pair.right();
		// int countPair = c.pairCount(code1, code2);
		// if (countPair < minCount) {
		// continue;
		// }
		// Symbol sym = t.symbols[code1];
		// Symbol sym2 = t.symbols[code2];
		// if (sym.length() < 8) {
		// // symbol has already max length: can not be suffixed
		// }
		// // if (
		// // // symbol 2has already max length: can not be suffixed
		// // sym2.length() < 8
		// // // concatEnd would give different merge result than concat
		// // && sym.length() + sym2.length() > 8) {
		// // Symbol merged = SymbolUtil.fsstConcatEnd(sym, sym2);
		// // addOrInc(candidates, merged, countPair);
		// // }
		// }
		// }
		return candidates;
	}

	private int minCount(int frac, boolean sampled) {
		// on first iteration, minCount is 1
		// on last iteration, minCount is 5
		int minCount = Math.max(config.getMinCountNumerator() * frac / config.getMinCountDenominator(), 1);
		assert minCount > 0;

		// TODO Should improve this else we may discord good symbols on very short training data (e.g. just a
		// simple String)
		if (frac >= 128 && !sampled) {
			// If the input is very small, we should considered all inputs at last pass
			minCount = 0;
		}

		// Maptitle always requires 5
		// C++ increases the requirement on each pass
		// TODO What is the point of discarding as we sort at the end?
		// minCount = 5;
		return minCount;
	}

	/**
	 * 
	 * @param candidates
	 * @param sym
	 * @param count
	 *            is typically count of occurrence
	 */
	private void addOrInc(Map<SymCodeless, QSym> candidates, Symbol sym, int count) {
		SymCodeless key = new SymCodeless(sym.val, sym.length());

		// If coded, we write 1
		// if escaped, we write 2*length
		// This is the actual gain, not reduced by the cost
		// The cost will be evaluated in the final pass, to get ride of poor symbols
		// Why isn't there a minus, as we gain `symbol.length() - symbol mark`
		int gain = count * sym.length();

		if (candidates.containsKey(key)) {
			// Happens when merging 2 symbols hits an already known symbol
			// Happens when 1-byte are encountered both unencoded and coded
			gain += candidates.get(key).gain;
		}
		candidates.put(key, new QSym(sym, gain));
	}

	/**
	 * Converts String to UTF-8 byte[] and trains a {@link SymbolTable}
	 */
	public IFsstEncoding train(String... inputs) {
		byte[][] bytes = new byte[inputs.length][];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = inputs[i].getBytes(StandardCharsets.UTF_8);
		}
		return train(bytes);
	}

	/**
	 * Converts String to UTF-8 byte[] and trains a {@link SymbolTable}
	 */
	public IFsstEncoding trainOverStrings(List<String> inputs) {
		byte[][] bytes = new byte[inputs.size()][];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = inputs.get(i).getBytes(StandardCharsets.UTF_8);
		}
		return train(bytes);
	}

	private record SamplingResult(boolean isSampled, List<? extends IByteSlice> samples) {

	}

	/**
	 * makeSample assembles a ~16KB deterministic pseudo-random sample composed of 512-byte slices from the inputs to
	 * keep training fast yet representative.
	 */
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	public SamplingResult makeSample(FsstTrainerConfig config, List<IByteSlice> inputs) {
		int total = inputs.stream().filter(arr -> arr != null).mapToInt(IByteSlice::length).sum();
		if (total < config.getSampleTarget()) {
			// Input is small enough: no need to sample it
			return new SamplingResult(false, inputs);
		}

		byte[] buf = new byte[config.getSampleMaxSize()];
		List<IByteSlice> sample = new ArrayList<>(inputs.size());
		int pos = 0;
		long rng = SymbolUtil.fsstHash(config.getRngSeed());

		while (pos < buf.length) {
			// Pick a random input
			rng = SymbolUtil.fsstHash(rng);
			int idx = (int) Long.remainderUnsigned(rng, inputs.size());
			while (inputs.get(idx) == null || inputs.get(idx).length() == 0) {
				// Skip empty inputs
				idx = (idx + 1) % inputs.size();
			}

			// Pick a random chunk
			int numChunks = (inputs.get(idx).length() + config.getSampleLine() - 1) / config.getSampleLine();
			rng = SymbolUtil.fsstHash(rng);
			int off = config.getSampleLine() * (int) Long.remainderUnsigned(rng, numChunks);

			// Adjust end if input is short
			int n = Math.min(inputs.get(idx).length() - off, config.getSampleLine());
			if (pos + n > buf.length) {
				// Skip last chunk is growing too much
				break;
			}
			sample.add(inputs.get(idx).sub(off, n));
			pos += n;

			if (pos >= config.getSampleTarget()) {
				break;
			}
		}
		return new SamplingResult(true, sample);
	}

	/**
	 * Utility class for keys in candidates map.
	 */
	public record SymCodeless(long val, int length) {
	}

}
