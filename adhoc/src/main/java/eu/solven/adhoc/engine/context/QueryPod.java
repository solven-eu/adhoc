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
package eu.solven.adhoc.engine.context;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.IMeasureResolver;
import eu.solven.adhoc.engine.cache.GuavaQueryStepCache;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import eu.solven.adhoc.util.AdhocTime;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the living action doing a query, typically while being executed by an {@link CubeQueryEngine}
 * 
 * @author Benoit Lacelle
 * @see StandardQueryPreparator
 */
@Builder(toBuilder = true)
@Value
@Slf4j
public class QueryPod implements IHasQueryOptions, IMeasureResolver, IHasMeasures, IIsCancellable {
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
	ISliceFactory sliceFactory = RowSliceFactory.builder().build();

	@NonNull
	@Default
	IColumnsManager columnsManager = ColumnsManager.builder().build();

	@NonNull
	@Default
	// By default, we do not jump into a separate thread/executorService, hence we do not rely on
	// AdhocUnsafe.adhocCommonPool
	ListeningExecutorService executorService = MoreExecutors.newDirectExecutorService();

	@NonNull
	@Default
	IQueryStepCache queryStepCache = IQueryStepCache.noCache();

	/**
	 * Once turned to not-null, can not be nulled again.
	 */
	@Getter(AccessLevel.PROTECTED)
	AtomicReference<OffsetDateTime> refCancellationDate = new AtomicReference<>();

	/**
	 * On cancellation, all these listeners will be called, triggering the cancellation of any inner queries
	 */
	List<Runnable> cancellationListeners = new CopyOnWriteArrayList<>();

	@Override
	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (query.getOptions().contains(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)) {
			return this.forest.resolveIfRefOpt(measure)
					.orElseGet(() -> EmptyMeasure.builder().name(measure.getName()).build());
		} else {
			return this.forest.resolveIfRef(measure);
		}
	}

	@Override
	public OffsetDateTime getCancellationDate() {
		return refCancellationDate.get();
	}

	// Poor Design, but used by `StandardQueryPreparator.filterForest`
	@Override
	public Set<IMeasure> getMeasures() {
		return forest.getMeasures();
	}

	@Override
	public Set<IQueryOption> getOptions() {
		return query.getOptions();
	}

	public void cancel() {
		if (refCancellationDate.compareAndSet(null, now())) {
			log.info("Cancelled queryId={}", queryId);

			drainCancellationListeners();
		} else {
			Duration cancellationDelay = Duration.between(refCancellationDate.get(), now());
			log.info("Cancelled queryId={} (already cancelled since {})", queryId, cancellationDelay);
		}
	}

	protected void drainCancellationListeners() {
		Iterator<Runnable> cancellationIt = cancellationListeners.iterator();

		List<Runnable> executed = new ArrayList<>();

		while (cancellationIt.hasNext()) {
			Runnable listener = cancellationIt.next();

			safeExecuteListener(listener);

			// Collect if executed
			executed.add(listener);
		}

		// Remove so given listener is not considered anymore
		cancellationListeners.removeAll(executed);
	}

	/**
	 * Execute a cancellation listener. This should manage most Exception by not throwing, to let other listeners being
	 * executed.
	 * 
	 * @param listener
	 */
	protected void safeExecuteListener(Runnable listener) {
		// Execute the listener
		try {
			listener.run();
		} catch (RuntimeException e) {
			log.warn("Issue while executing a cancellation listener (%s)".formatted(listener), e);
		}
	}

	protected OffsetDateTime now() {
		return AdhocTime.now().atOffset(AdhocTime.zoneOffset());
	}

	@Override
	public boolean isCancelled() {
		return refCancellationDate.get() != null;
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	public static QueryPod forTable(ITableWrapper table) {
		return forTable(table, CubeQuery.builder().build());
	}

	@Deprecated(since = "Used for tests, or edge-cases")
	public static QueryPod forTable(ITableWrapper table, ICubeQuery query) {
		return QueryPod.builder()
				.table(table)
				.query(query)
				.forest(MeasureForest.empty())
				.queryId(AdhocQueryId.builder().cube(table.getName()).build())
				.build();
	}

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	public static class QueryPodBuilder {
		ICubeQuery query;
		AdhocQueryId queryId;
		IMeasureForest forest;
		ITableWrapper table;
		ISliceFactory sliceFactory;

		// https://projectlombok.org/features/Builder
		// columnsManager is problematic as it has @Default
		// https://stackoverflow.com/questions/47883931/default-value-in-lombok-how-to-init-default-with-both-constructor-and-builder
		IColumnsManager columnsManager;
		// boolean columnsManager$set;

		// executorService is problematic as it has @Default
		ListeningExecutorService executorService;

		// executorService is problematic as it has @Default
		IQueryStepCache queryStepCache;

		public QueryPodBuilder columnsManager(IColumnsManager columnsManager) {
			this.columnsManager = columnsManager;

			return this;
		}

		public QueryPodBuilder executorService(ListeningExecutorService executorService) {
			this.executorService = executorService;

			return this;
		}

		public QueryPodBuilder queryStepCache(IQueryStepCache queryStepCache) {
			this.queryStepCache = queryStepCache;

			return this;
		}

		public QueryPod build() {
			if (table == null) {
				throw new IllegalStateException("table must not be null");
			} else if (query == null) {
				throw new IllegalStateException("table must not be null");
			}
			if (queryId == null) {
				queryId = AdhocQueryId.from(table.getName(), query);
			}
			if (columnsManager == null) {
				columnsManager = ColumnsManager.builder().build();
			}
			if (sliceFactory == null) {
				sliceFactory = AdhocFactoriesUnsafe.factories.getSliceFactoryFactory().makeFactory();
			}
			if (executorService == null) {
				// By default, we do not jump into a separate thread/executorService, hence we do not rely on
				// AdhocUnsafe.adhocCommonPool
				executorService = MoreExecutors.newDirectExecutorService();
			}
			if (queryStepCache == null) {
				queryStepCache = GuavaQueryStepCache.withSize(1);
			}

			return new QueryPod(query,
					queryId,
					forest,
					table,
					sliceFactory,
					columnsManager,
					executorService,
					queryStepCache);
		}
	}

	// This should be called only once per queryPod, as the queryId id would changed on each call.
	// Should this be cached?
	public QueryPod asTableQuery() {
		AdhocQueryId tableQueryId = AdhocQueryId.builder()
				.cube(getTable().getName())
				.parentQueryId(getQueryId().getQueryId())
				.cubeElseTable(false)
				.build();
		return this.toBuilder().queryId(tableQueryId).build();
	}

	@Override
	public void addCancellationListener(Runnable runnable) {
		if (refCancellationDate.get() == null) {
			log.trace("Register an additional cancellation listener");
			cancellationListeners.add(runnable);

			if (refCancellationDate.get() != null) {
				// This should be a rare race-condition
				log.warn("A cancellationListener was added during the cancellation process");
				drainCancellationListeners();
			}
		} else {
			// We're already cancelled: cancel right away
			log.trace("Execute on registration a cancellation listener");
			runnable.run();
		}
	}

	@Override
	public void removeCancellationListener(Runnable runnable) {
		cancellationListeners.remove(runnable);
	}

}
