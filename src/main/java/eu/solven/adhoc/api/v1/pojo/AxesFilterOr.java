package eu.solven.adhoc.api.v1.pojo;

import java.util.List;

import eu.solven.adhoc.api.v1.IAxesFilter;
import eu.solven.adhoc.api.v1.filters.IAxesFilterAnd;
import eu.solven.adhoc.api.v1.filters.IAxesFilterOr;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation for {@link IAxesFilterAnd}
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class AxesFilterOr implements IAxesFilterOr {

	@Singular
	final List<IAxesFilter> filters;

	@Override
	public boolean isExclusion() {
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
	public List<IAxesFilter> getOr() {
		return filters;
	}

}