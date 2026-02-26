package eu.solven.adhoc.util.immutable;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.encoding.page.row.ILikeList;
import lombok.experimental.UtilityClass;

/**
 * Helps working with immutable data-structures
 * 
 * @author Benoit Lacelle
 * 
 */
@UtilityClass
public class ImmutableHelpers {

	/**
	 * 
	 * @param map
	 * @return an immutable copy of the input, which may or may not be an {@link IAdhocMap}
	 */
	public static Map<String, ?> copyOf(Map<String, ?> map) {
		if (map instanceof IImmutable) {
			// For performance, we expect to be generally in this branch
			// IAdhocMap are immutable but they do not extend ImmutableMap
			return map;
		} else {
			return ImmutableMap.copyOf(map);
		}
	}

	@SuppressWarnings("unchecked") // all supported methods are covariant
	public static <T> List<T> copyOf(Iterable<? extends T> iterable) {
		if (iterable instanceof ILikeList likeList) {
			// For performance, we expect to be generally in this branch
			return ImmutableList.copyOf(likeList.asList());
		} else {
			return ImmutableList.copyOf(iterable);
		}
	}
}
