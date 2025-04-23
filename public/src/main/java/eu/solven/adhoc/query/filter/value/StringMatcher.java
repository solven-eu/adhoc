package eu.solven.adhoc.query.filter.value;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Useful to transcode from Pivotable/JSON as String to a known value with given type.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class StringMatcher implements IValueMatcher {
	@NonNull
	String string;

	@Override
	public boolean match(Object value) {
		if (value == null) {
			return false;
		} else {
			return string.equals(value.toString());
		}
	}

	public static IValueMatcher hasToString(Object hasToString) {
		String string = String.valueOf(hasToString);
		return StringMatcher.builder().string(string).build();
	}
}
