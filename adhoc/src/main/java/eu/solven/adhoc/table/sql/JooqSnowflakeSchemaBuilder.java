package eu.solven.adhoc.table.sql;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.jooq.Condition;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class helps building a star/snowflake schema, around some baseStore and various LEFT JOINs to provide additional columns.
 * <p>
 * This also facilitate the registration of proper transcoder. This implementation give priority to the first encounter of a field, giving priority to the LEFT table over the RIGHT table.
 */
//@Builder
public class JooqSnowflakeSchemaBuilder {
    // JOINs often refers the same name from the joined tables: this map will record the qualified field to refer when the unqualified field is queried
    // e.g. if `FROM table t JOIN joined j ON t.k = j.k`, then may decide that `k` always refers to `t.k`
    @NonNull
//    @Builder.Default
    @Getter
    final Map<String, String> queriedToUnderlying = new ConcurrentHashMap<>();

    @NonNull
    final Table<Record> baseTable ;
    @NonNull
    final String baseTableAlias;

    @Getter
    Table<Record> snowflakeTable;

    // https://stackoverflow.com/questions/30717640/how-to-exclude-property-from-lombok-builder
    // `snowflakeTable` is not built by the builder
    @Builder
    public JooqSnowflakeSchemaBuilder( Table<Record> baseTable, String baseTableAlias) {
//        this.queriedToUnderlying=queriedToUnderlying;
        this.baseTable=baseTable;
        this.baseTableAlias=baseTableAlias;

        this.snowflakeTable = baseTable.as(baseTableAlias);
    }

    /**
     * Assumes the LEFT table is the base store.
     *
     * @param joinedTable the JOINed table
     * @param joinName    the alias of the JOINed table
     * @param on          a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
     * @return
     */
    public JooqSnowflakeSchemaBuilder leftJoin(Table<?> joinedTable, String joinName, List<Map.Entry<String, String>> on) {
        return leftJoin(baseTableAlias, joinedTable, joinName, on);
    }

    /**
     * @param leftTableAlias the name of the LEFT/base table. May not be the root/base table given we manage snowflake schema.
     * @param joinedTable    the JOINed table
     * @param joinName       the alias of the JOINed table
     * @param on             a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
     * @return
     */
    public JooqSnowflakeSchemaBuilder leftJoin(String leftTableAlias, Table<?> joinedTable, String joinName, List<Map.Entry<String, String>> on) {
        List<Condition> onConditions = on.stream().map(e -> {
            Name leftName;
            if (e.getKey().contains(".")) {
                // We assume we received a qualified path in its String form
                // This would fail if the field was actually a field with a `.` in it
                leftName = DSL.quotedName(e.getKey().split("\\."));
            } else {
                leftName = DSL.quotedName(leftTableAlias, e.getKey());
            }
            return DSL.field(leftName).eq(DSL.field(DSL.quotedName(joinName, e.getValue())));
        }).toList();

        // Register in the transcoder
        on.forEach(fromTo -> {
            registerInTranscoder(leftTableAlias, joinName, fromTo);
        });

        return leftJoinConditions(joinedTable.as(joinName), onConditions);
    }

    public JooqSnowflakeSchemaBuilder joinHomo(String leftTableAlias, Table<?> joinedTable, String joinName, List<String> on) {
return leftJoin(leftTableAlias, joinedTable, joinName, on.stream().map(f -> Map.entry(f,f)).toList());
    }

    protected void registerInTranscoder(String leftTableAlias, String joinTable, Map.Entry<String, String> fromTo) {
        // `putIfAbsent`: priority to the first occurrence of the field
        // `leftTableAlias`; priority to the LEFT/base table than the RIGHT/joined table
        queriedToUnderlying.putIfAbsent(fromTo.getKey(), DSL.quotedName(leftTableAlias, fromTo.getKey()).toString());
    }

    public JooqSnowflakeSchemaBuilder leftJoinConditions(Table<?> joinedTable, List<Condition> on) {
        snowflakeTable = snowflakeTable.leftJoin(joinedTable)
                .on(
                        on.toArray(Condition[]::new));

        return this;
    }

    public static class  JooqSnowflakeSchemaBuilderBuilder {

        // https://github.com/projectlombok/lombok/issues/2307#issuecomment-1119511303
        private JooqSnowflakeSchemaBuilderBuilder snowflakeTable(Table<Record> snowflakeTable){
            return this;
        }
    }

}
