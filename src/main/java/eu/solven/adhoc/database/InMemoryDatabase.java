package eu.solven.adhoc.database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import eu.solven.adhoc.execute.FilterHelpers;
import eu.solven.adhoc.query.DatabaseQuery;
import lombok.Builder;
import lombok.Builder.Default;

@Builder
public class InMemoryDatabase implements IAdhocDatabase {
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

	public void add(Map<String, Object> row) {
		rows.add(row);
	}

	public Stream<Map<String, ?>> stream() {
		return rows.stream();
	}

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
		return this.stream().filter(row -> {
			return FilterHelpers.match(dbQuery.getFilter(), row);
		}).map(row -> {
			// BEWARE Should we filter not selected columns?
			Set<String> keysToKeep = new HashSet<>();

			keysToKeep.addAll(dbQuery.getGroupBy().getGroupedByColumns());

			dbQuery.getAggregators().stream().map(a -> a.getColumnName()).forEach(keysToKeep::add);

			return row;
		});
	}
}
