package eu.solven.adhoc.transformers;

import java.util.Comparator;
import java.util.Set;

import eu.solven.adhoc.dag.AdhocMeasuresSet;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * This is useful to refer to an existing {@link IMeasure} in the {@link AdhocMeasuresSet}, hence preventing to need to
 * provide its definition.
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
	public Set<String> getTags() {
		return Set.of("reference");
	}

	@Override
	public int compareTo(ReferencedMeasure o) {
		if (this.getClass() != o.getClass()) {
			return this.getClass().getName().compareTo(o.getClass().getName());
		}

		return Comparator.comparing(ReferencedMeasure::getRef).compare(this, o);
	}
}
