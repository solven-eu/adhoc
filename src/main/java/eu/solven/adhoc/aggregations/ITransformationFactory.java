package eu.solven.adhoc.aggregations;

/**
 * Provides {@link ITransformation} given their key. This can be extended to provides custom transformations.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITransformationFactory {
	ITransformation fromKey(String key);
}
