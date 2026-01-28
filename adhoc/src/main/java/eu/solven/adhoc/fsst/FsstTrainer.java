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
package eu.solven.adhoc.fsst;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.experimental.UtilityClass;

/**
 * Builds a static symbol table for FSST-style compression.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public final class FsstTrainer implements IFsstConstants {

	public static SymbolTable train(byte[] data) {
		// 1. Count substrings
		Object2IntMap<ByteArrayKey> freq = countSubstrings(data);

		return buildTable(freq);
	}

	public static SymbolTable train(Iterator<byte[]> data) {
		// 1. Count substrings
		Object2IntMap<ByteArrayKey> freq = makeEmptyMap();

		data.forEachRemaining(FsstTrainer::countSubstrings);

		return buildTable(freq);
	}

	@SuppressWarnings("PMD.LooseCoupling")
	private static Object2IntMap<ByteArrayKey> makeEmptyMap() {
		Object2IntOpenHashMap<ByteArrayKey> freq = new Object2IntOpenHashMap<>();
		freq.defaultReturnValue(-1);
		return freq;
	}

	private static SymbolTable buildTable(Object2IntMap<ByteArrayKey> freq) {
		// 2. Score substrings by compression gain
		List<Candidate> unsortedCandidates = scoreCandidates(freq);

		// 3. Pick best symbols
		List<Candidate> candidates = unsortedCandidates.stream()
				.sorted(Comparator.comparingInt(Candidate::gain).reversed())
				// Can not register more symbols
				.limit(MAX_SYMBOLS)
				.toList();

		SymbolTable table = new SymbolTable();
		for (Candidate c : candidates) {
			table.addSymbol(c.bytes);
		}

		return table;
	}

	// ----------------------------------------------------------------------

	private static Object2IntMap<ByteArrayKey> countSubstrings(byte[] data) {
		Object2IntMap<ByteArrayKey> freq = makeEmptyMap();
		countSubstrings(data, freq);
		return freq;
	}

	/**
	 * Given an input, we count the frequency of occurrence of each symbol, i.e. each substring.
	 * 
	 * @param data
	 * @param freq
	 */
	private static void countSubstrings(byte[] data, Object2IntMap<ByteArrayKey> freq) {
		if (data == null) {
			return;
		}

		for (int i = 0; i < data.length; i++) {
			for (int len = MIN_LEN; len <= MAX_LEN; len++) {
				if (i + len > data.length) {
					break;
				}

				ByteArrayKey key = ByteArrayKey.write(data, i, len);
				freq.merge(key, 1, Integer::sum);
			}
		}
	}

	/**
	 * For each symbol candidate, we evaluate its gain. Typically, we gain the number of a occurence of a symbol,
	 * multiplied by the difference between the original length and the symbol length.
	 * 
	 * @param freq
	 * @return
	 */
	private static List<Candidate> scoreCandidates(Object2IntMap<ByteArrayKey> freq) {
		List<Candidate> out = new ArrayList<>();

		for (var e : freq.object2IntEntrySet()) {
			int len = e.getKey().length();
			int count = e.getIntValue();

			// Gain formula from fsst paper/impl:
			// replacing len bytes by 1 byte
			int gain = count * (len - 1);

			if (gain > 0) {
				out.add(new Candidate(e.getKey(), gain));
			}
		}
		return out;
	}

	/**
	 * A symbol candidate, attached to its gain given its frequency.
	 */
	public record Candidate(ByteArrayKey bytes, int gain) {
	}

}