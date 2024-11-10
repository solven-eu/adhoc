package eu.solven.adhoc.api.v1.filters;

import java.util.List;

import eu.solven.adhoc.api.v1.IAdhocFilter;

public interface IAxesFilterAnd extends IAdhocFilter {

	/**
	 * Would throw if .isAnd is false
	 * 
	 * @return
	 */
	List<IAdhocFilter> getAnd();
}