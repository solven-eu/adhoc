package eu.solven.adhoc.map.perfect_hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SimplePerfectHash {

	private final String[] table;
	private final int modulus;

	public SimplePerfectHash(Set<String> keys) {
		int n = keys.size();
		List<String> keyList = new ArrayList<>(keys);

		int m = n;
		while (true) {
			String[] candidate = new String[m];
			boolean collision = false;

			for (String k : keyList) {
				int idx = Math.floorMod(k.hashCode(), m);
				if (candidate[idx] != null) {
					collision = true;
					break;
				}
				candidate[idx] = k;
			}

			if (!collision) {
				this.table = candidate;
				this.modulus = m;
				return;
			}

			m++; // try next size
		}
	}

	/**
	 * @param key
	 * @return the index in the original {@link Set}, or -1.
	 */
	public int indexOf(String key) {
		int idx = Math.floorMod(key.hashCode(), modulus);
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
		return Math.floorMod(key.hashCode(), modulus);
	}
}