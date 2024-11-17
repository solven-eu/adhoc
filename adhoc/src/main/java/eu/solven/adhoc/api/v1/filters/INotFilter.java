package eu.solven.adhoc.api.v1.filters;

import eu.solven.adhoc.api.v1.IAdhocFilter;

public interface INotFilter extends IAdhocFilter {
	IAdhocFilter getNegated();
}