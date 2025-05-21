package eu.solven.adhoc.table;

import com.google.common.collect.ImmutableList;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * An {@link ITableWrapper} which will returns result from a main {@link ITableWrapper}, but it will also send the request to alternative {@link ITableWrapper}.
 *
 * It is useful for development purposes, e.g. to check the output from `EXPLAIN` for each fork.
 */
@Builder
@Slf4j
public class ForkingTableWrapper implements ITableWrapper {
    @NonNull
    ITableWrapper main;

    @NonNull
    @Singular
    ImmutableList<ITableWrapper> forks;

    @Override
    public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
        ITabularRecordStream mainStream = this.main.streamSlices(queryPod, tableQuery);

        List<ITabularRecordStream> forkedStreams = forks.stream().map(fork -> fork.streamSlices(queryPod, tableQuery)).toList();


        return new ITabularRecordStream() {
            @Override
            public boolean isDistinctSlices() {
                return mainStream.isDistinctSlices();
            }

            @Override
            public Stream<ITabularRecord> records() {
                forkedStreams.forEach(forkedStream -> {
                    List<ITabularRecord> forkedRecords = forkedStream.records().toList();
                    log.info("Fork to {} returned {} rows", forkedStream, forkedRecords.size());
                });

                return mainStream.records();
            }

            @Override
            public void close() {
                mainStream.close();

                forkedStreams.forEach(ITabularRecordStream::close);
            }
        };
    }

    @Override
    public Collection<ColumnMetadata> getColumns() {
        return List.of();
    }

    @Override
    public String getName() {
        return "";
    }
}
