package eu.solven.adhoc.api.v1.pojo;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.regex.Pattern;

/**
 * To be used with {@link ColumnFilter}, for equality-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class EqualsMatcher implements IValueMatcher {
	@NonNull
	Object operand;

	@Override
	public boolean match(Object value) {
		return operand == value || operand.equals(value);
	}
}
