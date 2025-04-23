package eu.solven.adhoc.table.transcoder.value;

import eu.solven.adhoc.query.filter.value.IValueMatcher;

public interface ICustomTypeManagerSimple {
	/**
	 *
	 * @param column
	 * @param coordinate
	 *            some coordinate, typically queried by a cube/measure/user
	 * @return the equivalent object compatible with the underlying table
	 */
	Object toTable(String column, Object coordinate);

	/**
	 * This is especially useful to skip column with {@link eu.solven.adhoc.query.filter.value.IValueMatcher} which may
	 * not be supported
	 * 
	 * @param column
	 * @return true if this column may be transcoded
	 */
	boolean mayTranscode(String column);

	IValueMatcher toTable(String column, IValueMatcher valueMatcher);
}
