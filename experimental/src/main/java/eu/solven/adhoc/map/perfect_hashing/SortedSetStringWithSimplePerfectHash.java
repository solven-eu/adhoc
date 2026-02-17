package eu.solven.adhoc.map.perfect_hashing;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedSet;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Builder
public class SortedSetStringWithSimplePerfectHash implements ISortedSetString {
	@Getter
	@Singular
	final ImmutableSortedSet<String> keys;
	final Supplier<SimplePerfectHash> hash = Suppliers.memoize(() -> {
		return new SimplePerfectHash(getKeys());
	});

	@Override
	public int indexOf(String key) {
		return (int) hash.get().indexOf(key);
	}

	@Override
	public int unsafeIndexOf(String key) {
		return (int) hash.get().unsafeIndexOf(key);
	}
}
