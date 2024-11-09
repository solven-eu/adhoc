package eu.solven.adhoc.transformers;

import eu.solven.adhoc.dag.DAG;

/**
 * A node in a DAG of measures. Typically an {@link Aggregator} or a {@link Combinator}
 * 
 * @author Benoit Lacelle
 *
 */
// https://dax.guide/st/measure/
public interface IMeasure {

	/**
	 * 
	 * @return the name of the {@link IMeasure}. It has to be unique within a given {@link DAG}.
	 */
	String getName();

}
