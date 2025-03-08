package eu.solven.adhoc.dag;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.IAggregatedRecord;
import eu.solven.adhoc.slice.SliceAsMap;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class AggregatedRecordLogger {
	final AtomicInteger nbIn = new AtomicInteger();
	final AtomicInteger nbOut = new AtomicInteger();

	final String table;

	// https://stackoverflow.com/questions/25168660/why-is-not-java-util-stream-streamclose-called
	@Deprecated(since = "Not called automatically")
	public Runnable closeHandler() {
		return () -> {
			log.info("Aggregates from table completed accepting {} rows and rejecting {} rows (table={})",
					nbIn.get(),
					nbOut.get(),
					table);
		};
	}

	public BiConsumer<IAggregatedRecord, Optional<SliceAsMap>> prepareStreamLogger(TableQuery tableQuery) {

		BiConsumer<IAggregatedRecord, Optional<SliceAsMap>> peekOnCoordinate = (input, optCoordinates) -> {

			if (optCoordinates.isEmpty()) {
				// Skip this input as it is incompatible with the groupBy
				// This may not be done by IAdhocDatabaseWrapper for complex groupBys.
				// TODO Wouldn't this be a bug in IAdhocDatabaseWrapper?
				int currentOut = nbOut.incrementAndGet();
				if (tableQuery.isDebug() && Integer.bitCount(currentOut) == 1) {
					log.info("Rejected row #{}: {} (table={})", currentOut, input, table);
				}
			} else {
				int currentIn = nbIn.incrementAndGet();
				if (tableQuery.isDebug() && Integer.bitCount(currentIn) == 1) {
					log.info("Accepted row #{}: {} (table={})", currentIn, input, table);
				}
			}
		};
		return peekOnCoordinate;
	}

}
