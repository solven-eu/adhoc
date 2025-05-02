/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.dag.context;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.ICanResolveMeasure;
import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the living action doing a query, typically while being executed by an {@link AdhocQueryEngine}
 * 
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@Value
@Slf4j
public class ExecutingQueryContext implements IIsExplainable, IIsDebugable, IHasQueryOptions, ICanResolveMeasure {
	// The query requested to the queryEngine
	@NonNull
	ICubeQuery query;

	@NonNull
	AdhocQueryId queryId;

	// an IAdhocQuery is executed relatively to a measureBag as requested measure depends (implicitly) on underlying
	// measures
	@NonNull
	IMeasureForest forest;

	@NonNull
	ITableWrapper table;

	@NonNull
	@Default
	IColumnsManager columnsManager = ColumnsManager.builder().build();

	// Using a ForkJoinPool is much more complex than using an ExecutorService
	// But it enable smooth usage of Stream API.
	// Given part of the query is actually waiting for an external database (i.e. ITableQuery), it may be preferably not
	// to rely on the commonPool.
	@NonNull
	@Default
	ExecutorService fjp = AdhocUnsafe.adhocCommonPool; // ForkJoinPool.commonPool();

	/**
	 * Once turned to nut-null, can not be nulled again.
	 */
	AtomicReference<OffsetDateTime> cancellationDate = new AtomicReference<>();

	@Override
	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (query.getOptions().contains(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)) {
			if (measure instanceof ReferencedMeasure ref) {
				return this.forest.resolveIfRefOpt(ref)
						.orElseGet(() -> EmptyMeasure.builder().name(ref.getRef()).build());
			} else {
				return this.forest.resolveIfRefOpt(measure)
						.orElseGet(() -> EmptyMeasure.builder().name(measure.getName()).build());
			}
		} else {
			return this.forest.resolveIfRef(measure);
		}
	}

	@Override
	public boolean isExplain() {
		return getQuery().isExplain();
	}

	@Override
	public boolean isDebug() {
		return getQuery().isDebug();
	}

	@Override
	public Set<IQueryOption> getOptions() {
		return query.getOptions();
	}

	public void cancel() {
		if (cancellationDate.compareAndSet(null, now())) {
			log.info("Cancelled queryId={}", queryId);
		} else {
			Duration cancellationDelay = Duration.between(cancellationDate.get(), now());
			log.info("Cancelled queryId={} (already cancelled since {})", queryId, cancellationDelay);
		}
	}

	protected OffsetDateTime now() {
		return OffsetDateTime.now();
	}

	public boolean isCancelled() {
		return cancellationDate.get() != null;
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	public static ExecutingQueryContext forTable(ITableWrapper table) {
		return ExecutingQueryContext.builder()
				.table(table)
				.query(CubeQuery.builder().build())
				.forest(MeasureForest.empty())
				.queryId(AdhocQueryId.builder().cube(table.getName()).build())
				.build();
	}

	public static class ExecutingQueryContextBuilder {
		ICubeQuery query;
		AdhocQueryId queryId;
		IMeasureForest forest;
		ITableWrapper table;

		// https://projectlombok.org/features/Builder
		// columnsManager is problematic as it has @Default
		// https://stackoverflow.com/questions/47883931/default-value-in-lombok-how-to-init-default-with-both-constructor-and-builder
		IColumnsManager columnsManager;
		// boolean columnsManager$set;

		ExecutorService executorService;
		ForkJoinPool fjp;

		public ExecutingQueryContextBuilder columnsManager(IColumnsManager columnsManager) {
			this.columnsManager = columnsManager;

			return this;
		}

		public ExecutingQueryContext build() {
			if (queryId == null) {
				queryId = AdhocQueryId.from(table.getName(), query);
			}
			if (columnsManager == null) {
				columnsManager = ColumnsManager.builder().build();
			}
			// if (executorService == null) {
			// executorService = MoreExecutors.newDirectExecutorService();
			// }
			if (fjp == null) {
				fjp = ForkJoinPool.commonPool();
			}

			return new ExecutingQueryContext(query, queryId, forest, table, columnsManager
			// , executorService
					, fjp);
		}
	}

}
