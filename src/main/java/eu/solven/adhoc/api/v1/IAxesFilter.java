package eu.solven.adhoc.api.v1;

public interface IAxesFilter {
	/**
	 * If true, this {@link IAxesFilter} defines points to exclude.
	 * 
	 * If false, this {@link IAxesFilter} defines points to include.
	 * 
	 * @return
	 */
	boolean isExclusion();

	default boolean isMatchAll() {
		return false;
	}

	/**
	 * These are the most simple and primitive filters.
	 * 
	 * @return true if this filters an axis for a given value
	 */
	default boolean isAxisEquals() {
		return false;
	}

	default boolean isOr() {
		return false;
	}

	default boolean isAnd() {
		return false;
	}
}