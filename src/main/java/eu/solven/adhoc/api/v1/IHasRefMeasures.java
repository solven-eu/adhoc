package eu.solven.adhoc.api.v1;

import java.util.List;
import java.util.Set;

import eu.solven.adhoc.transformers.ReferencedMeasure;

/**
 * A {@link List} of {@link IMeasuredAxis}. Typically used by {@link IAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasRefMeasures {

	Set<ReferencedMeasure> getMeasures();
}