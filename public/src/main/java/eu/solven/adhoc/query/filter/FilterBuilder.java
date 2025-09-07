package eu.solven.adhoc.query.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import lombok.RequiredArgsConstructor;

/**
 * Enables creating {@link ISliceFilter} instances, enabling to skip or not optimizations.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class FilterBuilder {
	final List<ISliceFilter> filters = new ArrayList<>();

	final boolean andElseOr;

	public static FilterBuilder and() {
		return new FilterBuilder(true);
	}

	public static FilterBuilder and(Collection<? extends ISliceFilter> filters) {
		return and().filters(filters);
	}

	public static FilterBuilder and(ISliceFilter first, ISliceFilter... more) {
		return and().filters(first, more);
	}

	public static FilterBuilder or() {
		return new FilterBuilder(false);
	}

	public static FilterBuilder or(Collection<? extends ISliceFilter> filters) {
		return or().filters(filters);
	}

	public static FilterBuilder or(ISliceFilter first, ISliceFilter... more) {
		return or().filters(first, more);
	}

	public FilterBuilder filters(Collection<? extends ISliceFilter> filters) {
		this.filters.addAll(filters);

		return this;
	}

	public FilterBuilder filters(ISliceFilter first, ISliceFilter... more) {
		this.filters.add(first);
		this.filters.addAll(Arrays.asList(more));

		return this;
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} which is optimized.
	 */
	public ISliceFilter optimize() {
		if (andElseOr) {
			return FilterOptimizerHelpers.and(filters);
		} else {
			return OrFilter.or(filters);
		}
	}

	/**
	 * 
	 * @return a {@link ISliceFilter} skipping most expensive optimizations.
	 */
	public ISliceFilter combine() {
		if (andElseOr) {
			return AndFilter.builder().filters(filters).build();
		} else {
			return OrFilter.builder().filters(filters).build();
		}
	}
}
