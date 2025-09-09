package eu.solven.adhoc.query.filter;

import java.util.Collection;

/**
 * Holds the optimization of {@link ISliceFilter}
 * 
 * @author Benoit Lacelle
 */
public interface IFilterOptimizerHelpers {

	/**
	 * 
	 * @param filters
	 * @param willBeNegated
	 *            true if this expression will be negated (e.g. when being called by `OR`)
	 * @return
	 */
	ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated);

	ISliceFilter or(Collection<? extends ISliceFilter> filters);

}
