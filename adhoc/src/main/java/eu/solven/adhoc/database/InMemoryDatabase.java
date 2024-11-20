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
import lombok.NonNull;
import lombok.Getter;
import lombok.NonNull;

@Builder
public class InMemoryDatabase implements IAdhocDatabaseWrapper {

    @Default
    @NonNull
    @Getter
    final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

    @NonNull
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

    public void add(Map<String, ?> row) {
        rows.add(row);
    }

    protected Stream<Map<String, ?>> stream() {
        return rows.stream();
    }

    protected Map<String, ?> transcode(Map<String, ?> underlyingMap) {
        return AdhocTranscodingHelper.transcode(transcoder, underlyingMap);
    }

    @Override
    public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
        return this.stream().filter(row -> {
            return FilterHelpers.match(transcoder, dbQuery.getFilter(), row);
        }).map(row -> {
            // BEWARE Should we filter not selected columns?
            Set<String> keysToKeep = new HashSet<>();

            keysToKeep.addAll(dbQuery.getGroupBy().getGroupedByColumns());

            dbQuery.getAggregators().stream().map(a -> a.getColumnName()).forEach(keysToKeep::add);

            return row;
        }).map(this::transcode);
    }
}
