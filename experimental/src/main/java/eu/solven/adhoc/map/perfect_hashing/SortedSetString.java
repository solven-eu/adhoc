package eu.solven.adhoc.map.perfect_hashing;

import java.util.Collections;

import com.google.common.collect.ImmutableSortedSet;

import lombok.Builder;
import lombok.Singular;

@Builder
public class SortedSetString implements ISortedSetString {
	@Singular
	final ImmutableSortedSet<String> keys;

	@Override
	public int indexOf(String key) {
		return Collections.binarySearch(keys.asList(), key);
	}
}
