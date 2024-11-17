package eu.solven.adhoc.aggregations;

import java.util.Map;

import eu.solven.adhoc.transformers.IHasCombinationKey;

/**
 * Provides {@link ICombination} given their key. This can be extended to provides custom transformations.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IOperatorsFactory {

	default ICombination makeTransformation(IHasCombinationKey hasTransformationKey) {
		return makeCombination(hasTransformationKey.getCombinationKey(),
				hasTransformationKey.getCombinationOptions());
	}

	ICombination makeCombination(String key, Map<String, ?> options);

	IAggregation makeAggregation(String key);

	IDecomposition makeDecomposition(String key, Map<String, ?> decompositionOptions);
}
