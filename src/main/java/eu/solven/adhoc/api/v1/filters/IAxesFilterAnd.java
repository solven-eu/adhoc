package eu.solven.adhoc.api.v1.filters;

import java.util.List;

import eu.solven.adhoc.api.v1.IAxesFilter;

public interface IAxesFilterAnd extends IAxesFilter {

	/**
	 * Would throw if .isAnd is false
	 * 
	 * @return
	 */
	List<IAxesFilter> getAnd();
}