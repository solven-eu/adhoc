package eu.solven.adhoc.api.v1.filters;

import java.util.List;

import eu.solven.adhoc.api.v1.IAdhocFilter;

public interface IAndFilter extends IAdhocFilter {

	/**
	 * Would throw if .isAnd is false
	 * 
	 * @return
	 */
	List<IAdhocFilter> getAnd();
}