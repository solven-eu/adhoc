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
package eu.solven.adhoc.eventbus;

import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.dag.step.AdhocQueryStep;

/**
 * Describe each event which may be submitted by Adhoc.
 * 
 * An alternative is to just listen for {@link IAdhocEvent}
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocEventsListener {

	void onQueryLifecycleEvent(QueryLifecycleEvent event);

	/**
	 * Refers to {@link IAdhocQueryEngine} main phases.
	 * 
	 * @param event
	 */
	void onAdhocQueryPhaseIsCompleted(AdhocQueryPhaseIsCompleted event);

	/**
	 * Given a tree of {@link AdhocQueryStep}, this inform each step being processed.
	 * 
	 * @param event
	 */
	void onQueryStepIsEvaluating(QueryStepIsEvaluating event);

	/**
	 * An {@link eu.solven.adhoc.query.cube.AdhocQuery} is resolved through a DAG of
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep}. This will log when an
	 * {@link eu.solven.adhoc.dag.step.AdhocQueryStep} is completed.
	 * 
	 * @param event
	 */
	void onQueryStepIsCompleted(QueryStepIsCompleted event);

	/**
	 * Some message to be submitted to the logging layer.
	 * 
	 * @param event
	 */
	void onAdhocLogEvent(AdhocLogEvent event);

}
