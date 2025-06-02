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
package eu.solven.adhoc.engine.observability;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable overview of active-queries.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class AdhocQueryMonitor {
	public static final String TAG_QUERY_LIFECYCLE = "QUERY_LIFECYCLE";
	public static final String TAG_QUERY_START = "QUERY_START";
	public static final String TAG_QUERY_DONE = "QUERY_DONE";

	private static final int DEFAULT_MAX_SLOW_QUERIES = 100;

	// TODO Is it a leak to reference the whole context?
	protected final Map<QueryPod, OffsetDateTime> queryToStart = new ConcurrentHashMap<>();

	protected final int slowestQueriedMax;
	// TODO Is it a leak to reference the whole context?
	protected final BlockingQueue<Map.Entry<QueryPod, Duration>> slowestQueried;

	public AdhocQueryMonitor() {
		this(DEFAULT_MAX_SLOW_QUERIES);
	}

	public AdhocQueryMonitor(int maxSlowQueries) {
		// `+1` as we need to insert an additional query before removing the faster one
		slowestQueriedMax = maxSlowQueries;
		slowestQueried = new PriorityBlockingQueue<>(slowestQueriedMax + 1, comparatorForSlowest());
	}

	public void onQueryLifecycleEvent(QueryLifecycleEvent lifecycleEvent) {
		QueryPod query = lifecycleEvent.getQuery();

		int nbActive;
		synchronized (query) {
			if (lifecycleEvent.getTags().contains(TAG_QUERY_START)) {
				OffsetDateTime removed = queryToStart.put(query, now());

				if (removed != null) {
					log.warn("Received start event but query already started: {}", lifecycleEvent);
				}

				nbActive = queryToStart.size();
			} else if (lifecycleEvent.getTags().contains(TAG_QUERY_DONE)) {
				OffsetDateTime removed = queryToStart.remove(query);

				if (removed == null) {
					log.warn("Received done event but query is not registered: {}", lifecycleEvent);
				} else {
					Duration duration = Duration.between(removed, now());
					slowestQueried.add(Map.entry(query, duration));

					if (slowestQueried.size() > slowestQueriedMax) {
						Map.Entry<QueryPod, Duration> slowestRemoved = slowestQueried.remove();
						log.debug("Not amongst slowest anymore: {}", slowestRemoved);
					}
				}
				nbActive = queryToStart.size();

			} else {
				nbActive = -1;
			}
		}

		if (nbActive > 0) {
			onNbActive(nbActive);
		}
	}

	private Comparator<Map.Entry<QueryPod, Duration>> comparatorForSlowest() {
		Comparator<Entry<QueryPod, Duration>> comparingByValue = Map.Entry.comparingByValue();

		return comparingByValue
		// We want first entry with large duration
		// In fact, we want fast to be first, to be dropped on `.remove()`
		// .reversed()
		;
	}

	protected void onNbActive(int nbActive) {
		if (nbActive == 0 || Integer.bitCount(nbActive) == 1) {
			log.info("nbActive={}", nbActive);
		}
	}

	protected OffsetDateTime now() {
		return OffsetDateTime.now();
	}
}
