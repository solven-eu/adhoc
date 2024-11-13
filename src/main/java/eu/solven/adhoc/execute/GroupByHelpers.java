package eu.solven.adhoc.execute;

import java.util.NavigableSet;
import java.util.TreeSet;

import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.query.GroupByColumns;
import lombok.NonNull;

public class GroupByHelpers {

	public static IAdhocGroupBy union(IAdhocGroupBy left, @NonNull IAdhocGroupBy right) {
		NavigableSet<String> union = new TreeSet<>();

		union.addAll(left.getGroupedByColumns());
		union.addAll(right.getGroupedByColumns());

		if (union.isEmpty()) {
			return GroupByColumns.GRAND_TOTAL;
		} else {
			return GroupByColumns.builder().groupBy(union).build();
		}
	}

}
