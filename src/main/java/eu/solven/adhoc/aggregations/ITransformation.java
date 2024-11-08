package eu.solven.adhoc.aggregations;

import java.util.List;

import eu.solven.adhoc.transformers.Combinator;

/**
 * An {@link ITransformation} can turn a {@link List} of values (typically from {@link Combinator}) into a new value.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITransformation {
	Object transform(List<Object> underlyingValues);
}
