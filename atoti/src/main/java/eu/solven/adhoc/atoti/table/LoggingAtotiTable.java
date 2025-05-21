package eu.solven.adhoc.atoti.table;

import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.IGetAggregatesQuery;
import com.quartetfs.biz.pivot.query.impl.GetAggregatesQuery;
import com.quartetfs.fwk.query.IQuery;
import com.quartetfs.fwk.query.IQueryable;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * This simulates a ActivePivot/Atoti instance receiving {@link IGetAggregatesQuery}. Can be used to log about the received {@link IGetAggregatesQuery}.
 */
@Slf4j
public class LoggingAtotiTable extends AAdhocAtotiTable implements  IQueryable{

    @NonNull
    @Getter
    final String pivotId;

    @Builder
    public LoggingAtotiTable(String pivotId, ITableTranscoder transcoder) {
        super(transcoder);
        this.pivotId = pivotId;
    }

    @Override
    protected IActivePivotVersion inferPivotId() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IQueryable inferQueryable() {
        return this;
    }

    @Override
    public Set<String> getSupportedQueries() {
        return Set.of(GetAggregatesQuery.PLUGIN_KEY);
    }

    @Override
    public <ResultType> ResultType execute(IQuery<ResultType> query)  {
        if (query instanceof IGetAggregatesQuery gaq) {
            log.info("pivot={} received gaq={}", getPivotId(), gaq);

            return (ResultType) new CellSetDTO();
        } else {
            throw new UnsupportedOperationException("query=%s".formatted(query));
        }
    }
}
