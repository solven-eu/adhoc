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

import com.google.common.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

/**
 * This logs main steps of the query-engine. It is typically activated by calling `#AdhocQueryBuilder.debug()`.
 *
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel {
	/**
	 * An {@link eu.solven.adhoc.query.cube.AdhocQuery} is resolved through a DAG of
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep}. This will log when an
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep} is completed.
	 * 
	 * @param event
	 */
	@Subscribe
	public void onQueryStepIsCompleted(QueryStepIsCompleted event) {
		log.debug("size={} for queryStep={} on completed (source={})",
				event.getNbCells(),
				event.getQuerystep(),
				event.getSource());
	}

	@Subscribe
	public void onAdhocQueryPhaseIsCompleted(AdhocQueryPhaseIsCompleted event) {
		log.debug("query phase={} is completed (source={})", event.getPhase(), event.getSource());
	}

	@Subscribe
	public void onQueryStepIsEvaluating(QueryStepIsEvaluating event) {
		log.debug("queryStep={} is evaluating (source={})", event.getQueryStep(), event.getSource());
	}

	@Subscribe
	public void onExplainOrDebugEvent(AdhocLogEvent event) {
		log.debug("{}{} {} (source={})",
				event.isDebug() ? "[DEBUG]" : "",
				event.isExplain() ? "[EXPLAIN]" : "",
				event.getMessage(),
				event.getSource());
	}

}
