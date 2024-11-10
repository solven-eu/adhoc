package eu.solven.adhoc.query;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GroupByColumns implements IAdhocGroupBy {
	NavigableSet<String> groupBy;

	@Override
	public NavigableSet<String> getGroupedByColumns() {
		return groupBy;
	}

	public static IAdhocGroupBy of(Collection<String> wildcards) {
		return GroupByColumns.builder().groupBy(new TreeSet<>(wildcards)).build();
	}

}
