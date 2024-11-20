package eu.solven.adhoc.api.v1;

import java.util.Collections;

import eu.solven.adhoc.api.v1.pojo.AndFilter;

public interface IAdhocFilter {
	IAdhocFilter MATCH_ALL = new AndFilter(Collections.emptyList());

	/**
	 * If true, this {@link IAdhocFilter} defines points to exclude.
	 * 
	 * If false, this {@link IAdhocFilter} defines points to include.
	 * 
	 * @return
	 */
	default boolean isNot() {
		return false;
	}

	default boolean isMatchAll() {
		return false;
	}

	/**
	 * These are the most simple and primitive filters.
	 * 
	 * @return true if this filters a column for a given value
	 */
	default boolean isColumnMatcher() {
		return false;
	}

	default boolean isOr() {
		return false;
	}

	default boolean isAnd() {
		return false;
	}
}