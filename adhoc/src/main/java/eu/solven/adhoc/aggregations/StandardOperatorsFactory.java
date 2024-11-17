package eu.solven.adhoc.aggregations;

import java.util.Map;

import eu.solven.adhoc.aggregations.max.MaxAggregator;
import eu.solven.adhoc.aggregations.max.MaxTransformation;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;

public class StandardOperatorsFactory implements IOperatorsFactory {

	@Override
	public ICombination makeCombination(String key, Map<String, ?> options) {
		return switch (key) {
		case SumCombination.KEY: {
			yield new SumCombination();
		}
		case MaxTransformation.KEY: {
			yield new MaxTransformation();
		}
		case DivideCombination.KEY: {
			yield new DivideCombination();
		}
		case ExpressionCombination.KEY: {
			yield ExpressionCombination.parse(options);
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

	@Override
	public IAggregation makeAggregation(String key) {
		return switch (key) {
		case SumAggregator.KEY: {
			yield new SumAggregator();
		}
		case MaxAggregator.KEY: {
			yield new MaxAggregator();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

	@Override
	public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
		return switch (key) {
		case AdhocIdentity.KEY: {
			yield new AdhocIdentity();
		}
		case LinearDecomposition.KEY: {
			yield new LinearDecomposition(options);
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

}
