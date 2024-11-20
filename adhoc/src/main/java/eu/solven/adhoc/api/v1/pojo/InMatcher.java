package eu.solven.adhoc.api.v1.pojo;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Set;

/**
 * To be used with {@link ColumnFilter}, for IN-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class InMatcher implements IValueMatcher {
	@NonNull
	Set<?> operands;

	@Override
	public boolean match(Object value) {
		if (operands.contains(value)) {
			return true;
		}

		if (operands.stream().anyMatch(operand -> operand instanceof IValueMatcher vm && vm.match(value))) {
			return true;
		}

		return false;
	}
}
