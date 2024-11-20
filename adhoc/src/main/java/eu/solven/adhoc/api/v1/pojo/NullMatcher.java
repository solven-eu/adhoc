package eu.solven.adhoc.api.v1.pojo;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * To be used with {@link ColumnFilter}, for null-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class NullMatcher implements IValueMatcher {
	/**
	 * This enables managing null not through a null-reference.
	 * TODO: Clarify its serialization behavior.
	 */
//	private static final Object NULL_MARKER = new Object();
//
//	public static @NonNull Object nullMarker() {
//		return NULL_MARKER;
//	}

	public static @NonNull IValueMatcher matchNull() {
		return NullMatcher.builder().build();
	}

	@Override
	public boolean match(Object value) {
		return value == null;
	}
}
