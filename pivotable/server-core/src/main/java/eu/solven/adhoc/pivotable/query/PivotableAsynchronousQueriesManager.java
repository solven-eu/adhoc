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
package eu.solven.adhoc.pivotable.query;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.DisposableBean;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.util.IHasCache;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Manage queries being executed asynchronously.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class PivotableAsynchronousQueriesManager implements IHasCache, DisposableBean {
	// This is useful in unitTest, not to be confused in case of concurrent tests
	private static final AtomicInteger INDEX = new AtomicInteger();

	final ListeningExecutorService asynchronousQueriesService =
			MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(Thread.ofPlatform()
					// Daemon as these thread does not hold critical operations
					.daemon(true)
					.name("pivotable-asynchronous-", INDEX.getAndIncrement())
					.factory()));

	final PivotableAsynchronousQueriesPolicy policy = PivotableAsynchronousQueriesPolicy.builder().build();

	final Cache<UUID, AsynchronousStatus> queryIdToState = CacheBuilder.newBuilder()
			// The state remains available 10x longer than the view
			.expireAfterWrite(policy.getExpireAfterWrite().multipliedBy(policy.getFactorForState()))
			.maximumSize(policy.getMaxSize())
			.build();

	final Cache<UUID, ListenableFuture<ITabularView>> queryIdToFuture = CacheBuilder.newBuilder().build();

	final Cache<UUID, Throwable> queryIdToThrowable = CacheBuilder.newBuilder()
			// The state remains available 10x longer than the view
			.expireAfterWrite(policy.getExpireAfterWrite().multipliedBy(policy.getFactorForState()))
			.maximumSize(policy.getMaxSize())
			.build();

	final Cache<UUID, ITabularView> queryIdToView = CacheBuilder.newBuilder()
			// Give the UI a reasonable amount of time to fetch the result
			.expireAfterWrite(policy.getExpireAfterWrite())
			.maximumSize(policy.getMaxSize())
			.softValues()
			.removalListener(new RemovalListener<UUID, ITabularView>() {

				@Override
				public void onRemoval(RemovalNotification<UUID, ITabularView> notification) {
					UUID queryId = notification.getKey();
					log.info("Removed view for queryId={}", queryId);
					markAsDiscarded(queryId);
				}
			})
			.build();

	protected void markAsDiscarded(UUID queryId) {
		// BEWARE this resets the expiryToWrite
		queryIdToState.put(queryId, AsynchronousStatus.DISCARDED);
	}

	@SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
	public UUID execute(IAdhocSchema schema, TargetedCubeQuery queryOnSchema) {
		UUID queryId = generateQueryId();

		queryIdToState.put(queryId, AsynchronousStatus.RUNNING);

		ListenableFuture<ITabularView> future = schema.executeAsync(queryOnSchema.getCube(), queryOnSchema.getQuery());
		queryIdToFuture.put(queryId, future);

		future.addListener(() -> {
			try {
				queryIdToFuture.invalidate(queryId);

				ITabularView view = Futures.getUnchecked(future);
				log.info("Registered view for queryId={}", queryId);
				queryIdToView.put(queryId, view);
				// Mark as served after writing the result
				queryIdToState.put(queryId, AsynchronousStatus.SERVED);
			} catch (Throwable t) {
				if (t instanceof CancellationException || Throwables.getRootCause(t) instanceof InterruptedException) {
					log.info("queryId={} cancelled", queryId);
				} else {
					log.warn("queryId={} failed", queryId, t);
				}
				queryIdToState.put(queryId, AsynchronousStatus.FAILED);
				queryIdToThrowable.put(queryId, t);
			}
		}, asynchronousQueriesService);

		return queryId;
	}

	protected UUID generateQueryId() {
		return UUID.randomUUID();
	}

	/**
	 * Holds a consistent pair of a state and its view. Typically, it is never SERVED with no view.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class StateAndView {
		AsynchronousStatus state;
		@Default
		Optional<ITabularView> optView = Optional.empty();
		@Default
		Optional<String> stacktrace = Optional.empty();
	}

	public StateAndView getStateAndView(UUID queryId) {
		AsynchronousStatus state =
				Optional.ofNullable(queryIdToState.getIfPresent(queryId)).orElse(AsynchronousStatus.UNKNOWN);
		if (state == AsynchronousStatus.SERVED) {
			Optional<ITabularView> optView = getView(queryId);
			if (optView.isEmpty()) {
				log.info("Polling right during expiring of view. Bad-luck or bug?");

				// Make sure not to returned a SERVED with no view, as it would be confusing
				return StateAndView.builder().state(AsynchronousStatus.DISCARDED).optView(Optional.empty()).build();
			} else {
				return StateAndView.builder().state(state).optView(optView).build();
			}
		} else if (state == AsynchronousStatus.FAILED) {
			Throwable t = queryIdToThrowable.getIfPresent(queryId);
			if (t == null) {
				log.warn("Missing stacktrace for {} on queryId={}", state, queryId);
				return StateAndView.builder().state(state).build();
			} else {
				return StateAndView.builder()
						.state(state)
						.stacktrace(Optional.of(Throwables.getStackTraceAsString(t)))
						.build();
			}
		} else {
			return StateAndView.builder().state(state).build();
		}
	}

	protected Optional<ITabularView> getView(UUID queryId) {
		return Optional.ofNullable(queryIdToView.getIfPresent(queryId));
	}

	public AsynchronousStatus getState(UUID queryId) {
		return getStateAndView(queryId).getState();
	}

	@Override
	public void invalidateAll() {
		// We may want to keep the state, to in
		queryIdToView.invalidateAll();
		queryIdToState.invalidateAll();
		queryIdToThrowable.invalidateAll();
	}

	@Override
	public void destroy() {
		asynchronousQueriesService.close();
	}

	public CancellationStatus cancelQuery(UUID queryId) {
		ListenableFuture<ITabularView> future = queryIdToFuture.getIfPresent(queryId);
		if (future != null) {
			log.info("Cancelling queryId={}", queryId);
			future.cancel(true);

			return CancellationStatus.CANCELLED;
		} else {
			log.info("Cancelling queryId={} but it is {}", queryId, CancellationStatus.UNKNOWN);
			return CancellationStatus.UNKNOWN;
		}
	}
}
