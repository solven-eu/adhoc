package eu.solven.adhoc.measure.model.lambda;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * Enable an {@link ICombination} to be defined through a lambda. Beware this is typically not serializable.
 * 
 * @author Benoit Lacelle
 */
public class LambdaCombination implements ICombination {
	public static final String K_LAMBDA = "lambda";

	@FunctionalInterface
	public interface ILambdaCombination extends BiFunction<ISliceWithStep, List<?>, Object> {

	}

	final ILambdaCombination lambda;

	public LambdaCombination(Map<String, ?> options) {
		lambda = MapPathGet.getRequiredAs(options, K_LAMBDA);
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		return lambda.apply(slice, underlyingValues);
	}
}