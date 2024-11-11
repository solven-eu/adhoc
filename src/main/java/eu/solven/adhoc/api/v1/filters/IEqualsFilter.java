package eu.solven.adhoc.api.v1.filters;

import javax.annotation.Nonnull;

import eu.solven.adhoc.api.v1.IAdhocFilter;

public interface IEqualsFilter extends IAdhocFilter {

	@Nonnull
	String getAxis();

	@Nonnull
	Object getFiltered();

}