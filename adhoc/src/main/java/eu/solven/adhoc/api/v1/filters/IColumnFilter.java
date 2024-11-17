package eu.solven.adhoc.api.v1.filters;

import java.util.Collection;

import javax.annotation.Nonnull;

import eu.solven.adhoc.api.v1.IAdhocFilter;

/**
 * Filter along a specific column. Typically for `=` or `IN` matchers.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IColumnFilter extends IAdhocFilter {

	/**
	 * 
	 * @return the name of the filtered column.
	 */
	@Nonnull
	String getColumn();

	/**
	 * The filter could be null, a {@link Collection} for a `IN` clause, else it is interpreted as an `=` clause.
	 * 
	 * BEWARE we would introduce a `LIKE` clause here.
	 * 
	 * @return the filtered value.
	 */
	Object getFiltered();

}