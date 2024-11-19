package eu.solven.adhoc.database;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.NonNull;
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
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class JooqSqlDatabase implements IAdhocDatabaseWrapper {
    @Builder.Default
    @NonNull
    @Getter
    final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

    @NonNull
    final Supplier<Connection> connectionSupplier;

    @NonNull
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

        Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(dbQuery);

        SelectConditionStep<Record> sqlQuery = makeDsl().select(selectedFields).from(DSL.name(tableName)).where(dbConditions);

        if (dbQuery.isExplain()) {
            log.info("[EXPLAIN] SQL to db: `{}`", sqlQuery.getSQL(ParamType.INLINED));
        }

        Stream<Map<String, ?>> dbStream = sqlQuery.stream().map(Record::intoMap);

        return dbStream.filter(row -> {
            // We could have a fallback, to filter manually when it is not doable by the DB (or we do not know how to
            // build the proper filter)
            return true;
        }).map(row -> {
            // In case of manual filters, we may have to hide some some columns, needed by the manual filter, but
            // unexpected by the output stream
            return transcode(row);
        });
    }

    private Collection<SelectFieldOrAsterisk> makeSelectedFields(DatabaseQuery dbQuery) {
        Collection<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
        dbQuery.getAggregators().stream().map(Aggregator::getColumnName).map(transcoder::underlying).distinct().forEach(c -> selectedFields.add(DSL.field(c)));

        dbQuery.getGroupBy().getGroupedByColumns().stream().map(transcoder::underlying).distinct().forEach(c -> selectedFields.add(DSL.field(c)));
        return selectedFields;
    }

    protected Map<String, ?> transcode(Map<String, ?> underlyingMap) {
        Map<String, Object> transcoded = new HashMap<>();

        underlyingMap.forEach((underlyingKey, v) -> {
            String queriedKey = transcoder.queried(underlyingKey);
            Object replaced = transcoded.put(queriedKey, v);

            if (replaced != null && !replaced.equals(v)) {
                log.warn("Transcoding led to an ambiguity as multiple underlyingKeys has queriedKey={} mapping to values {} and {}", queriedKey, replaced, v);
            }
        });

        return transcoded;
    }

    protected Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
        // We're interested in a row if at least one measure is not null
        List<Condition> oneNotNullConditions = aggregators.stream().map(Aggregator::getColumnName).map(c -> DSL.field(c).isNotNull()).collect(Collectors.toList());

        return DSL.or(oneNotNullConditions);
    }

    public DSLContext makeDsl() {
        return DSL.using(connectionSupplier.get());
    }

    protected Condition toCondition(IAdhocFilter filter) {
        if (filter.isAxisEquals() && filter instanceof IColumnFilter equalsFilter) {
            Condition condition;
            Object filtered = equalsFilter.getFiltered();
            String column = transcoder.underlying(equalsFilter.getColumn());
            if (filtered == null) {
                condition = DSL.condition(DSL.field(column).isNull());
            } else if (filtered instanceof Collection<?> filteredCollection) {
                condition = DSL.condition(DSL.field(column).in(filteredCollection));
//			} else if (filtered instanceof LikeF<?> filteredCollection) {
//				condition = DSL.condition(DSL.field(column).in(filteredCollection));
            } else {
                condition = DSL.condition(DSL.field(column).eq(filtered));
            }
            return condition;
        } else {
            throw new UnsupportedOperationException("Not handled: " + filter);
        }
    }
}
