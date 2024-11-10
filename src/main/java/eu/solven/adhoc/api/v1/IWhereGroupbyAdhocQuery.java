package eu.solven.adhoc.api.v1;

import java.util.List;

/**
 * An {@link IWhereGroupbyAdhocQuery} is view of a query, not expressing its measures.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IWhereGroupbyAdhocQuery extends IHasFilters, IHasGroupBy {

	/**
	 * The filter of current query. A filter refers to the condition for the data to be included. An AND over an empty
	 * {@link List} means the whole data has to be included. Exclusions can be done through {@link IExclusionFilter}
	 * 
	 * @return a list of filters (to be interpreted as an OR over AND simple conditions).
	 */
	@Override
	IAdhocFilter getFilter();

	/**
	 * The columns amongst which the result has to be ventilated/sliced.
	 * 
	 * @return a Set of columns
	 */
	@Override
	IAdhocGroupBy getGroupBy();
}