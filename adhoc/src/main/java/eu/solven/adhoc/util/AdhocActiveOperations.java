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
package eu.solven.adhoc.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.AdhocQueryPhaseIsCompleted;
import eu.solven.adhoc.eventbus.IAdhocEventsListener;
import eu.solven.adhoc.eventbus.QueryLifecycleEvent;
import eu.solven.adhoc.eventbus.QueryStepIsCompleted;
import eu.solven.adhoc.eventbus.QueryStepIsEvaluating;
import eu.solven.adhoc.eventbus.TableStepIsCompleted;
import eu.solven.adhoc.eventbus.TableStepIsEvaluating;

/**
 * Used to report active operations, typically useful when some long operations is going on.
 * 
 * @deprecated Beware we may have multiple CubeQuery refers to the same TableQuery. Should rely on queryId?
 * 
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
public class AdhocActiveOperations implements IAdhocEventsListener {
	// maximumSize to prevent this leading to a leak
	@SuppressWarnings("checkstyle:MagicNumber")
	final Cache<Object, Throwable> eventToStartStack = CacheBuilder.newBuilder().maximumSize(1024).build();

	@Override
	public void onQueryLifecycleEvent(QueryLifecycleEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onAdhocQueryPhaseIsCompleted(AdhocQueryPhaseIsCompleted event) {
		eventToStartStack.put(event, new RuntimeException());
	}

	@Override
	public void onQueryStepIsEvaluating(QueryStepIsEvaluating event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onQueryStepIsCompleted(QueryStepIsCompleted event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onAdhocLogEvent(AdhocLogEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTableStepIsEvaluating(TableStepIsEvaluating event) {
		eventToStartStack.put(event.getTableQuery(), new RuntimeException());
	}

	@Override
	public void onTableStepIsCompleted(TableStepIsCompleted event) {
		eventToStartStack.invalidate(event.getTableQuery());
	}
}
