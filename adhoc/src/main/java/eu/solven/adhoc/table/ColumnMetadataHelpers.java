package eu.solven.adhoc.table;

import java.util.ArrayList;
import java.util.List;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;

public class ColumnMetadataHelpers {

	public static CoordinatesSample getCoordinatesMostGeneric(IAdhocTableWrapper table,
			String column,
			IValueMatcher valueMatcher,
			int limit) {
		TableQuery tableQuery = TableQuery.builder().groupBy(GroupByColumns.named(column)).build();

		int returnedCoordinates;
		List<Object> distinctCoordinates;

		if (limit < 1) {
			returnedCoordinates = Integer.MAX_VALUE;
			distinctCoordinates = new ArrayList<>();
		} else {
			returnedCoordinates = limit;
			distinctCoordinates = new ArrayList<>(limit);
		}

		long estimatedCardinality = table.streamSlices(tableQuery)
				.asMap()
				.map(r -> r.getGroupBy(column))
				// TODO Should we return the information about null-ness?
				.filter(c -> c != null)
				// `.disinct()` is relevant only for InMemoryTable and related tables
				.distinct()
				.peek(coordinate -> {
					if (distinctCoordinates.size() < returnedCoordinates) {
						distinctCoordinates.add(coordinate);
					}
				})
				.count();

		return CoordinatesSample.builder()
				.coordinates(distinctCoordinates)
				.estimatedCardinality(estimatedCardinality)
				.build();
	}
}
