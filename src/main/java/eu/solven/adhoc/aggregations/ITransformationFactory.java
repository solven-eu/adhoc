package eu.solven.adhoc.aggregations;

import java.util.Map;

import eu.solven.adhoc.transformers.IHasTransformationKey;

/**
 * Provides {@link ITransformation} given their key. This can be extended to provides custom transformations.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITransformationFactory {

	default ITransformation makeTransformation(IHasTransformationKey hasTransformationKey) {
		return makeTransformation(hasTransformationKey.getTransformationKey(),
				hasTransformationKey.getTransformationOptions());
	}

	ITransformation makeTransformation(String key, Map<String, ?> options);

	IAggregation makeAggregation(String key);
}
