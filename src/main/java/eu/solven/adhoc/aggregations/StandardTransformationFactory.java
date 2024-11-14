package eu.solven.adhoc.aggregations;

import java.util.Map;

public class StandardTransformationFactory implements ITransformationFactory {

	@Override
	public ITransformation makeTransformation(String key, Map<String, ?> options) {
		return switch (key) {
		case SumTransformation.KEY: {
			yield new SumTransformation();
		}
		case MaxTransformation.KEY: {
			yield new MaxTransformation();
		}
		case DivideTransformation.KEY: {
			yield new DivideTransformation();
		}
		case ExpressionTransformation.KEY: {
			yield ExpressionTransformation.parse(options);
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
