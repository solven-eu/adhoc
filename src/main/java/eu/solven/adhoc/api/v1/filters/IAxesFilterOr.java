package eu.solven.adhoc.api.v1.filters;

import java.util.List;

import eu.solven.adhoc.api.v1.IAxesFilter;

public interface IAxesFilterOr extends IAxesFilter {

	/**
	 * Would throw if .isOr is false
	 * 
	 * @return
	 */
	List<IAxesFilter> getOr();
}