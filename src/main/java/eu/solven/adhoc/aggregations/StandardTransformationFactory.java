package eu.solven.adhoc.aggregations;

public class StandardTransformationFactory implements ITransformationFactory {

	@Override
	public ITransformation fromKey(String key) {
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
		default:
			throw new IllegalArgumentException("Unexpected value: " + key);
		};
	}

}
