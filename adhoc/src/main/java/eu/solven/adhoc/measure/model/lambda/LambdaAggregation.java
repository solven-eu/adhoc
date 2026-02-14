package eu.solven.adhoc.measure.model.lambda;

import java.util.Map;
import java.util.function.BiFunction;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * Enable an {@link IAggregation} to be defined through a lambda. Beware this is typically not serializable.
 * 
 * @author Benoit Lacelle
 */
public class LambdaAggregation implements IAggregation {
	public static final String K_LAMBDA = "lambda";

	@FunctionalInterface
	public interface ILambdaAggregation extends BiFunction<Object, Object, Object> {

	}

	final ILambdaAggregation lambda;

	public LambdaAggregation(Map<String, ?> options) {
		lambda = MapPathGet.getRequiredAs(options, K_LAMBDA);
	}

	@Override
	public Object aggregate(Object left, Object right) {
		return lambda.apply(left, right);
	}
}