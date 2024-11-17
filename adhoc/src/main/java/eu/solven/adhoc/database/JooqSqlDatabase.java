package eu.solven.adhoc.database;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectConditionStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.transformers.Aggregator;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link DatabaseQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class JooqSqlDatabase implements IAdhocDatabaseWrapper {
	final Supplier<Connection> connectionSupplier;

	final String tableName;

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
		Collection<Condition> dbConditions = new ArrayList<>();

		dbConditions.add(oneMeasureIsNotNull(dbQuery.getAggregators()));

		IAdhocFilter filter = dbQuery.getFilter();
		if (!filter.isMatchAll()) {
			if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
				andFilter.getAnd().stream().map(this::toCondition).forEach(dbConditions::add);
			} else {
				dbConditions.add(toCondition(filter));
			}
		}

		Collection<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		dbQuery.getAggregators()
				.stream()
				.map(a -> a.getColumnName())
				.distinct()
				.forEach(c -> selectedFields.add(DSL.field(c)));

		dbQuery.getGroupBy().getGroupedByColumns().stream().distinct().forEach(c -> selectedFields.add(DSL.field(c)));

		SelectConditionStep<Record> sqlQuery =
				makeDsl().select(selectedFields).from(DSL.name(tableName)).where(dbConditions);

		if (dbQuery.isExplain()) {
			log.info("[EXPLAIN] SQL to db: `{}`", sqlQuery.getSQL(ParamType.INLINED));
		}

		Stream<Map<String, ?>> dbStream = sqlQuery.stream().map(r -> r.intoMap());

		return dbStream.filter(row -> {
			// We could have a fallback, to filter manually when it is not doable by the DB (or we do not know how to
			// build the proper filter)
			return true;
		}).map(row -> {
			// In case of manual filters, we may have to hide some some columns, needed by the manual filter, but
			// unexpected by the output stream
			return row;
		});
	}

	private Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
		// We're interested in a row if at least one measure is not null
		List<Condition> oneNotNullConditions = aggregators.stream()
				.map(a -> a.getColumnName())
				.map(c -> DSL.field(c).isNotNull())
				.collect(Collectors.toList());

		return DSL.or(oneNotNullConditions);
	}

	public DSLContext makeDsl() {
		return DSL.using(connectionSupplier.get());
	}

	private Condition toCondition(IAdhocFilter filter) {
		if (filter.isAxisEquals() && filter instanceof IColumnFilter equalsFilter) {
			Condition condition;
			Object filtered = equalsFilter.getFiltered();
			if (filtered == null) {
				condition = DSL.condition(DSL.field(equalsFilter.getColumn()).isNull());
			} else if (filtered instanceof Collection<?> filteredCollection) {
				condition = DSL.condition(DSL.field(equalsFilter.getColumn()).in(filteredCollection));
			} else {
				condition = DSL.condition(DSL.field(equalsFilter.getColumn()).eq(filtered));
			}
			return condition;
		} else {
			throw new UnsupportedOperationException("Not handled: " + filter);
		}
	}
}
