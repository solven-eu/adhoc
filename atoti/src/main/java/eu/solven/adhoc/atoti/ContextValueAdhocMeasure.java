package eu.solven.adhoc.atoti;

import eu.solven.adhoc.transformers.IMeasure;

import java.util.Set;

/**
 * This demonstrates how one can add custom elements to the {@link eu.solven.adhoc.dag.AdhocQueryStep}.
 */
public class ContextValueAdhocMeasure implements IMeasure {
    @Override
    public String getName() {
        return "";
    }

    @Override
    public Set<String> getTags() {
        return Set.of();
    }
}
