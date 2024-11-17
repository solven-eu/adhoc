package eu.solven.adhoc.api.v1;

import java.util.Collections;
import java.util.NavigableSet;

import lombok.ToString;

@ToString
public class GrandTotal implements IAdhocGroupBy {

	@Override
	public boolean isGrandTotal() {
		return true;
	}

	@Override
	public NavigableSet<String> getGroupedByColumns() {
		return Collections.emptyNavigableSet();
	}

}
