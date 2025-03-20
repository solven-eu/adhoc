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
package eu.solven.adhoc.dag;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.column.IAdhocColumnsManager;
import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.model.EmptyMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
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
public class ExecutingQueryContext implements IIsExplainable, IIsDebugable {
	// The query requested to the queryEngine
	@NonNull
	IAdhocQuery query;

	@NonNull
	@Default
	AdhocQueryId queryId = AdhocQueryId.builder().build();

	// an IAdhocQuery is executed relatively to a measureBag as requested measure depends (implicitly) on underlying
	// measures
	@NonNull
	IAdhocMeasureBag measures;

	@NonNull
	IAdhocTableWrapper table;

	@NonNull
	@Default
	final IAdhocColumnsManager columnsManager = AdhocColumnsManager.builder().build();

	@NonNull
	@Singular
	Set<? extends IQueryOption> options;

	AtomicReference<OffsetDateTime> cancellationDate = new AtomicReference<>();

	protected IMeasure resolveIfRef(IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (options.contains(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)) {
			if (measure instanceof ReferencedMeasure ref) {
				return this.measures.resolveIfRefOpt(ref)
						.orElseGet(() -> EmptyMeasure.builder().name(ref.getRef()).build());
			} else {
				return this.measures.resolveIfRefOpt(measure)
						.orElseGet(() -> EmptyMeasure.builder().name(measure.getName()).build());
			}
		} else {
			return this.measures.resolveIfRef(measure);
		}
	}

	public boolean isExplain() {
		return getQuery().isExplain() || options.contains(StandardQueryOptions.EXPLAIN);
	}

	public boolean isDebug() {
		return getQuery().isDebug() || options.contains(StandardQueryOptions.DEBUG);
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
}
