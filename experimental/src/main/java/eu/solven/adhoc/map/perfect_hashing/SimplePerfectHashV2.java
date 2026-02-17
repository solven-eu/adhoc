package eu.solven.adhoc.map.perfect_hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SimplePerfectHashV2 {

	private final String[] table;
	private final int mask;

	public SimplePerfectHashV2(Set<String> keys) {
		int n = keys.size();
		List<String> keyList = new ArrayList<>(keys);

		// TODO ensure this return the same valu is already a power of two, else the following powerOfTwo
		int m = 1 << (32 - Integer.numberOfLeadingZeros(n - 1));

		while (true) {
			int mask = (1 << m) - 1;
			String[] candidate = new String[mask];
			boolean collision = false;

			for (String key : keyList) {

				int h = key.hashCode();
				// TODO Re-use fsst constant?
				h *= 0x9E3779B9;
				int idx = h & mask; // mask = m - 1
				if (candidate[idx] != null) {
					collision = true;
					break;
				}
				candidate[idx] = key;
			}

			if (!collision) {
				this.table = candidate;
				this.mask = mask;
				return;
			}

			// TODO We should try with various shift as it may have the same cost to shift the mask, and would help to
			// keep a smaller table
			m++; // try next size
		}
	}

	/**
	 * @param key
	 * @return the index in the original {@link Set}, or -1.
	 */
	public int indexOf(String key) {
		int h = key.hashCode();
		h *= 0x9E3779B9;
		int idx = h & mask; // mask = m - 1
		if (idx >= table.length || !key.equals(table[idx])) {
			return -1; // not found
		}
		return idx;
	}

	/**
	 * May be slightly faster than {@link #indexOf(String)}, but you need to ensure you're calling a valid key.
	 * 
	 * @param key
	 * @return the index in the original {@link Set}
	 */
	public int unsafeIndexOf(String key) {
		int h = key.hashCode();
		return (h * 0x9E3779B9) & mask; // mask = m - 1
	}
}