package eu.solven.adhoc.api.v1.pojo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

/**
 * Default implementation for {@link IAndFilter}
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@Builder
public class AndFilter implements IAndFilter {

	@Singular
	final List<IAdhocFilter> filters;

	public static AndFilter andAxisEqualsFilters(Map<String, ?> filters) {
		return new AndFilter(filters.entrySet()
				.stream()
				.map(e -> new EqualsFilter(e.getKey(), e.getValue()))
				.collect(Collectors.toList()));
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty AND is considered to match everything
		boolean empty = filters.isEmpty();

		if (empty) {
			return true;
		}

		if (filters.size() == 1 && filters.get(0).isMatchAll()) {
			return true;
		}

		return false;
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public List<IAdhocFilter> getAnd() {
		return Collections.unmodifiableList(filters);
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", filters.size());

		AtomicInteger index = new AtomicInteger();
		filters.stream().limit(5).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocFilter and(IAdhocFilter filter, IAdhocFilter... moreFilters) {
		return and(Lists.asList(filter, moreFilters));
	}

	public static IAdhocFilter and(List<? extends IAdhocFilter> filters) {
		return AndFilter.builder().filters(filters).build();
	}

}