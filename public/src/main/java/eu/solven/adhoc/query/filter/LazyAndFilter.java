package eu.solven.adhoc.query.filter;

import java.util.Set;

/**
 * An IAndFilter which does not optimize itself automatically. It should not be used as key (explicitly or implicitly
 * (e.g.through a CubeQueryStep)).
 */
@Deprecated(since = "Unclear if this is good design")
public class LazyAndFilter implements IAndFilter {

	@Override
	public Set<ISliceFilter> getOperands() {
		return Set.of();
	}

}
