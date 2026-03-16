package eu.solven.adhoc.table.sql;

import lombok.Builder;
import org.jooq.Record;
import org.jooq.ResultQuery;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Splits a query in multiple queries to take advantage of partitioning.
 *
 * BEWARE We need to evaluate when this is better than just letting ITableWrapper partition itself. If Adhoc has low concurrency, it may help streaming results from table, instead of waiting for one large result.
 *
 *
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not functional")
@Builder
public class ModuloQueryPartitionor implements IQueryPartitionor{
    private static final String FETCH_PARTITION_KEYS = """
            SELECT
                attname AS sortkey_column,
                a.attsortkeyord AS sortkey_order
            FROM
                pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE
                n.nspname = 'someSchema'
                AND c.relname = 'someTableName'
                AND a.attsortkeyord > 0
            ORDER BY a.attsortkeyord;
            """;

    @Builder.Default
    public int modulo = 4;

    @Override
    public List<ResultQuery<Record>> partition(ResultQuery<Record> query) {
        // `mod(abs(from_hex(substring(md5("someColumn"), 1, 16))::bigint), 4) = 0`
        return IntStream.range(0, modulo).mapToObj(index -> query).toList();
    }
}
