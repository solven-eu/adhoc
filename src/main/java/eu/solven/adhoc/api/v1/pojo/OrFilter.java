package eu.solven.adhoc.api.v1.pojo;

import java.util.List;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IOrFilter;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation for {@link IAndFilter}
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class OrFilter implements IOrFilter {

	@Singular
	final List<IAdhocFilter> filters;

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty OR is considered to match nothing
		return !filters.isEmpty();
	}

	@Override
	public boolean isOr() {
		return true;
	}

	@Override
	public List<IAdhocFilter> getOr() {
		return filters;
	}

}