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

	double aggregateDoubles(double left, double right);
	
	long aggregateLongs(long left, long right);
}
