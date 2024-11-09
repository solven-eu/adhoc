package eu.solven.adhoc.api.v1.filters;

import javax.annotation.Nonnull;

import eu.solven.adhoc.api.v1.IAxesFilter;

public interface IAxesFilterAxisEquals extends IAxesFilter {

	@Nonnull
	String getAxis();

	@Nonnull
	Object getFiltered();
}