package eu.solven.adhoc.transformers;

import java.util.Comparator;

import eu.solven.adhoc.dag.DAG;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * This is useful to refer to an existing {@link IMeasure} in the {@link DAG}, hence preventing to need to provide
 * its definition.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class ReferencedMeasure implements IMeasure, Comparable<ReferencedMeasure> {
	String ref;

	@Override
	public String getName() {
		return "ref-" + getRef();
	}

	@Override
	public int compareTo(ReferencedMeasure o) {
		if (this.getClass() != o.getClass()) {
			return this.getClass().getName().compareTo(o.getClass().getName());
		}

		return Comparator.comparing(ReferencedMeasure::getRef).compare(this, o);
	}
}
