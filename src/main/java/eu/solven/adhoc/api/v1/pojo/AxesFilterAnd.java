package eu.solven.adhoc.api.v1.pojo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAxesFilterAnd;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Default implementation for {@link IAxesFilterAnd}
 * 
 * @author Benoit Lacelle
 *
 */
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode
public class AxesFilterAnd implements IAxesFilterAnd {

	final List<IAdhocFilter> filters;

	public static AxesFilterAnd andAxisEqualsFilters(Map<String, ?> filters) {
		return new AxesFilterAnd(filters.entrySet()
				.stream()
				.map(e -> new AxisEqualsFilter(e.getKey(), e.getValue()))
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

}