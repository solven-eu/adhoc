package eu.solven.adhoc.transformers;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.dag.AdhocMeasureBag;

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
	 * @return the name of the {@link IMeasure}. It has to be unique within a given {@link AdhocMeasureBag}.
	 */
	String getName();

	/**
	 * Tags are useful for various operations, like documentation (e.g. coloring some graphviz by tag).
	 * 
	 * @return the tags applied to this measure.
	 */
	Set<String> getTags();

	// JsonIgnore as implied by tags
	@JsonIgnore
	default boolean isDebug() {
		return getTags().contains("debug");
	}

}
