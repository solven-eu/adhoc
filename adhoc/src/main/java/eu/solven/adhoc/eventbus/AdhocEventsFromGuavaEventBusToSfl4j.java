/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.eventbus;

import java.util.function.BiConsumer;

import com.google.common.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

/**
 * This logs main steps of the query-engine. It is typically activated by calling `#AdhocQueryBuilder.debug()`.
 *
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class AdhocEventsFromGuavaEventBusToSfl4j {

	// This must not have `@Subscribe`, else events would be processed multiple times
	// This is useful when the EventBus is not Guava
	public void onAdhocEvent(IAdhocEvent event) {
		if (event instanceof QueryStepIsCompleted queryStepIsCompleted) {
			onQueryStepIsCompleted(queryStepIsCompleted);
		} else if (event instanceof AdhocQueryPhaseIsCompleted queryPhaseIsCompleted) {
			onAdhocQueryPhaseIsCompleted(queryPhaseIsCompleted);
		} else if (event instanceof QueryStepIsEvaluating queryStepIsEvaluating) {
			onQueryStepIsEvaluating(queryStepIsEvaluating);
		} else if (event instanceof AdhocLogEvent logEvent) {
			onAdhocLogEvent(logEvent);
		} else {
			log.warn("Not managed properly: {}", event);
		}
	}

	/**
	 * An {@link eu.solven.adhoc.query.AdhocQuery} is resolved through a DAG of
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep}. This will log when an
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep} is completed.
	 * 
	 * @param event
	 */
	@Subscribe
	public void onQueryStepIsCompleted(QueryStepIsCompleted event) {
		log.info("size={} for queryStep={} on completed (source={})",
				event.getNbCells(),
				event.getQuerystep(),
				event.getSource());
	}

	@Subscribe
	public void onAdhocQueryPhaseIsCompleted(AdhocQueryPhaseIsCompleted event) {
		log.info("query phase={} is completed (source={})", event.getPhase(), event.getSource());
	}

	@Subscribe
	public void onQueryStepIsEvaluating(QueryStepIsEvaluating event) {
		log.info("queryStep={} is evaluating (source={})", event.getQueryStep(), event.getSource());
	}

	@Subscribe
	public void onAdhocLogEvent(AdhocLogEvent event) {
		BiConsumer<String, Object[]> logMethod;
		if (event.isWarn()) {
			logMethod = log::warn;
		} else {
			logMethod = log::info;
		}
		Object[] arguments = { event.isDebug() ? "[DEBUG]" : "",
				event.isExplain() ? "[EXPLAIN]" : "",
				event.getMessage(),
				event.getSource() };

		logMethod.accept("{}{} {} (source={})", arguments);
	}

}
