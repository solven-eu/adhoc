package eu.solven.adhoc.fsst;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * Builds a static symbol table for FSST-style compression.
 */
public final class FsstTrainer {

	private static final int MIN_LEN = 2;
	// Default is 8.
	private static final int MAX_LEN = 8;
	private static final int MAX_SYMBOLS = 128;

	private FsstTrainer() {
	}

	public static SymbolTable train(byte[] data) {
		// 1. Count substrings
		Object2IntMap<ByteArrayKey> freq = countSubstrings(data);

		return buildTable(freq);
	}

	public static SymbolTable train(Iterator<byte[]> data) {
		// 1. Count substrings
		Object2IntOpenHashMap<ByteArrayKey> freq = makeEmptyMap();

		data.forEachRemaining(FsstTrainer::countSubstrings);

		return buildTable(freq);
	}

	private static Object2IntOpenHashMap<ByteArrayKey> makeEmptyMap() {
		Object2IntOpenHashMap<ByteArrayKey> freq = new Object2IntOpenHashMap<>();
		freq.defaultReturnValue(-1);
		return freq;
	}

	private static SymbolTable buildTable(Object2IntMap<ByteArrayKey> freq) {
		// 2. Score substrings by compression gain
		List<Candidate> candidates = scoreCandidates(freq);

		// 3. Pick best symbols
		candidates.sort(Comparator.comparingInt(Candidate::gain).reversed());

		SymbolTable table = new SymbolTable();
		int code = 0;

		for (Candidate c : candidates) {
			if (code >= MAX_SYMBOLS) {
				// Can not register more symbols
				break;
			}
			table.addSymbol(c.bytes, code++);
		}

		return table;
	}

	// ----------------------------------------------------------------------

	private static Object2IntMap<ByteArrayKey> countSubstrings(byte[] data) {
		Object2IntOpenHashMap<ByteArrayKey> freq = makeEmptyMap();
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
				if (i + len > data.length)
					break;

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
				out.add(new Candidate(e.getKey(), len, gain));
			}
		}
		return out;
	}

	// ----------------------------------------------------------------------

	private record Candidate(ByteArrayKey bytes, int len, int gain) {
	}

}