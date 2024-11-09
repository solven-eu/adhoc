package eu.solven.adhoc.api.v1;

import java.util.List;

/**
 * An {@link IWhereGroupbyAdhocQuery} is view of a query, not expressing its measures.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IWhereGroupbyAdhocQuery extends IHasFilters, IHasGroupBys {

	/**
	 * The filters of current query. A filter refers to the condition for the data to be included. An empty {@link List}
	 * means the whole data has to be included. Exclusions can be done through {@link IExclusionFilter}
	 * 
	 * @return a list of filters (to be interpreted as an OR over AND simple conditions).
	 */
	@Override
	IAxesFilter getFilters();

	/**
	 * The columns amongst which the result has to be ventilated/sliced.
	 * 
	 * @return a Set of columns
	 */
	@Override
	List<String> getGroupBys();
}