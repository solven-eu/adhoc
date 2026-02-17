package eu.solven.adhoc.map.perfect_hashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedSet;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Builder
public class SortedSetStringWithPerfectHash implements ISortedSetString {
	@Getter
	@Singular
	final ImmutableSortedSet<String> keys;
	final Supplier<GOVMinimalPerfectHashFunction<String>> hash = Suppliers.memoize(() -> {
		try {
			return new GOVMinimalPerfectHashFunction.Builder<String>().keys(getKeys())
					.transform(TransformationStrategies.utf16())
					.build();
		} catch (IOException e) {
			throw new UncheckedIOException("Issue with " + getKeys(), e);
		}
	});

	@Override
	public int indexOf(String key) {
		return (int) hash.get().getLong(key);
	}
}
