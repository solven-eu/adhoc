package eu.solven.adhoc.api.v1.pojo;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * To be used with {@link ColumnFilter}, for equality-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class SameMatcher implements IValueMatcher {
	@NonNull
	Object operand;

	@Override
	public boolean match(Object value) {
		return operand == value ;
	}
}
