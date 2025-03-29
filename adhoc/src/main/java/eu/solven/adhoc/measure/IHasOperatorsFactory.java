package eu.solven.adhoc.measure;

import eu.solven.adhoc.dag.AdhocQueryEngine;

/**
 * For classes holding an {@link IOperatorsFactory}
 * 
 * @author Benoit Lacelle
 */
public interface IHasOperatorsFactory {
	IOperatorsFactory getOperatorsFactory();

	/**
	 * 
	 * @param o
	 *            typically a {@link AdhocQueryEngine}
	 * @return an {@link IOperatorsFactory}, a default one if the input does not implement {@link IHasOperatorsFactory}.
	 */
	static IOperatorsFactory getOperatorsFactory(Object o) {
		if (o instanceof IHasOperatorsFactory hasOperatorsFactory) {
			return hasOperatorsFactory.getOperatorsFactory();
		} else {
			return new StandardOperatorsFactory();
		}
	}
}
