package eu.solven.adhoc.map.perfect_hashing;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedSet;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Builder
public class SortedSetStringWithHashMap implements ISortedSetString {
	@Singular
	@Getter
	final ImmutableSortedSet<String> keys;

	final Supplier<Map<String, Integer>> hash = Suppliers.memoize(() -> {
		Map<String, Integer> asMap = new LinkedHashMap<>();

		getKeys().forEach(key -> asMap.put(key, asMap.size()));

		return asMap;
	});

	@Override
	public int indexOf(String key) {
		return hash.get().getOrDefault(key, -1);
	}
}
