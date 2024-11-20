package eu.solven.adhoc.aggregations;

import java.util.List;

import eu.solven.adhoc.transformers.Combinator;

/**
 * An {@link IAggregation} can turn a {@link List} of values (typically from {@link Combinator}) into a new value.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregation {
	Object aggregate(Object left, Object right);

	default String aggregateStrings(String left, String right) {
		Object aggregated = aggregate(left, right);

		if (aggregated == null) {
			return null;
		}

		return aggregated.toString();
	}

	double aggregateDoubles(double left, double right);
	
	long aggregateLongs(long left, long right);
}
