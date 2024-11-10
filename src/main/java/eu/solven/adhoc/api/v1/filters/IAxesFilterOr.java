package eu.solven.adhoc.api.v1.filters;

import java.util.List;

import eu.solven.adhoc.api.v1.IAdhocFilter;

public interface IAxesFilterOr extends IAdhocFilter {

	/**
	 * Would throw if .isOr is false
	 * 
	 * @return
	 */
	List<IAdhocFilter> getOr();
}