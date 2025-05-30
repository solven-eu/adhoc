package eu.solven.adhoc.column.generated_column;

import java.util.Map;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

public class EmptyColumnGenerator implements ICompositeColumnGenerator {

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return CoordinatesSample.empty();
	}

	/**
	 * Typically useful as default.
	 * 
	 * @return an empty {@link ICompositeColumnGenerator}.
	 */
	public static ICompositeColumnGenerator empty() {
		return new EmptyColumnGenerator();
	}

}
