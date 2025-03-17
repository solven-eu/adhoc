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

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdhocQueryMonitor {
	public static final String TAG_QUERY_LIFECYCLE = "QUERY_LIFECYCLE";
	public static final String TAG_QUERY_START = "QUERY_START";
	public static final String TAG_QUERY_DONE = "QUERY_DONE";

	final Map<ExecutingQueryContext, OffsetDateTime> queryToStart = new ConcurrentHashMap<>();

	public void onQueryStart(QueryLifecycleEvent lifecycleEvent) {
		ExecutingQueryContext query = lifecycleEvent.getQuery();

		int nbActive;
		synchronized (query) {
			if (lifecycleEvent.getTags().contains(TAG_QUERY_START)) {
				queryToStart.put(query, now());
				nbActive = queryToStart.size();
			} else if (lifecycleEvent.getTags().contains(TAG_QUERY_START)) {
				queryToStart.remove(query);
				nbActive = queryToStart.size();
			} else {
				nbActive = -1;
			}
		}

		if (nbActive > 0) {
			onNbActive(nbActive);
		}
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
