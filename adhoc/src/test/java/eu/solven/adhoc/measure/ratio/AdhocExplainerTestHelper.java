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
package eu.solven.adhoc.measure.ratio;

import java.util.ArrayList;
import java.util.List;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import eu.solven.adhoc.dag.observability.DagExplainer;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.util.AdhocUnsafe;

/**
 * Useful to test {@link DagExplainer}
 *
 * @author Benoit Lacelle
 */
public class AdhocExplainerTestHelper {
	protected AdhocExplainerTestHelper() {
		// hidden
	}

	public static List<String> listenForExplainNoPerf(EventBus eventBus) {
		AdhocUnsafe.resetDeterministicQueryIds();
		List<String> messages = new ArrayList<>();

		// Register an eventListener to collect the EXPLAIN results
		{
			Object listener = new Object() {

				@Subscribe
				public void onExplainOrDebugEvent(AdhocLogEvent event) {
					if (event.isExplain() && !event.isPerformance()) {
						messages.add(event.getMessage());
					}
				}
			};

			eventBus.register(listener);
		}

		return messages;
	}

	public static List<String> listenForPerf(EventBus eventBus) {
		AdhocUnsafe.resetDeterministicQueryIds();
		List<String> messages = new ArrayList<>();

		// Register an eventListener to collect the EXPLAIN results
		{
			Object listener = new Object() {

				@Subscribe
				public void onExplainOrDebugEvent(AdhocLogEvent event) {
					if (event.isPerformance()) {
						messages.add(event.getMessage());
					}
				}
			};

			eventBus.register(listener);
		}

		return messages;
	}

	public static List<String> listenForLogs(EventBus eventBus) {
		AdhocUnsafe.resetDeterministicQueryIds();
		List<String> messages = new ArrayList<>();

		// Register an eventListener to collect the EXPLAIN results
		{
			Object listener = new Object() {

				@Subscribe
				public void onExplainOrDebugEvent(AdhocLogEvent event) {
					messages.add(event.getMessage());
				}
			};

			eventBus.register(listener);
		}

		return messages;
	}

}
