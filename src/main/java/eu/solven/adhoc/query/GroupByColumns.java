package eu.solven.adhoc.query;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

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

	@Override
	public String toString() {
		if (isGrandTotal()) {
			return "grandTotal";
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", groupBy.size());

		AtomicInteger index = new AtomicInteger();
		groupBy.stream().limit(5).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocGroupBy of(Collection<String> wildcards) {
		return GroupByColumns.builder().groupBy(new TreeSet<>(wildcards)).build();
	}

}
