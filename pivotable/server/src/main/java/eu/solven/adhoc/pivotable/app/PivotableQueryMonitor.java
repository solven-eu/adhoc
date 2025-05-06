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
package eu.solven.adhoc.pivotable.app;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.greenrobot.eventbus.Subscribe;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.observability.AdhocQueryMonitor;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;

/**
 * Extends {@link AdhocQueryMonitor} for Pivotable custom needs (like receiving GreenBot events and providing JMX).
 * 
 * @author Benoit Lacelle
 */
@ManagedResource
public class PivotableQueryMonitor extends AdhocQueryMonitor {

	@Subscribe
	@Override
	public void onQueryLifecycleEvent(QueryLifecycleEvent lifecycleEvent) {
		super.onQueryLifecycleEvent(lifecycleEvent);
	}

	@ManagedAttribute
	public int getNbActive() {
		return this.queryToStart.size();
	}

	@ManagedAttribute
	public Map<String, Duration> getActiveToDuration() {
		Map<String, Duration> queryToStartForJmx = new LinkedHashMap<>();

		Comparator<Map.Entry<QueryPod, OffsetDateTime>> comparingByValue = Map.Entry.comparingByValue();
		Comparator<Map.Entry<QueryPod, OffsetDateTime>> comparingByValueR = comparingByValue.reversed();

		OffsetDateTime now = now();

		this.queryToStart.entrySet().stream().sorted(comparingByValueR).forEach(entry -> {
			QueryPod query = entry.getKey();
			OffsetDateTime start = entry.getValue();

			queryToStartForJmx.put(query.getQueryId().getQueryId() + " - " + query.getQuery().toString(),
					Duration.between(start, now));
		});

		return queryToStartForJmx;
	}
}
