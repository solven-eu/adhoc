package eu.solven.adhoc.map.perfect_hashing;

public interface ISortedSetString {
	int indexOf(String key);

	default int unsafeIndexOf(String key) {
		return indexOf(key);
	}
}
