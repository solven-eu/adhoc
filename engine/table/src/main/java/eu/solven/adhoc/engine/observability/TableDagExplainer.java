/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import lombok.Builder;
import lombok.Builder.Default;

/**
 * 
 * Atomic EXPLAIN emission: the header ("/-- N inducers from ...") and the per-step rows are collected into a single
 * buffer and posted as ONE AdhocLogEvent. Posting per-step events would let the SLF4J sink interleave rows from
 * concurrent table queries, scrambling the ASCII-art block. Same rationale and downstream contract as DagExplainer.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class TableDagExplainer {
	final Map<TableQueryStep, ICuboid> oneQueryStepToValues;

	@Default
	final String eol = AdhocEventsFromGuavaEventBusToSfl4j.EOL;

	@SuppressWarnings("PMD.AvoidStringBufferField")
	final StringBuilder explainLines = new StringBuilder();
	final AtomicInteger queryStepIndex = new AtomicInteger();

	public void header(String perfLog) {
		explainLines.append("/-- ").append(oneQueryStepToValues.size()).append(" inducers from ").append(perfLog);
	}

	public void step(String stepAsString) {
		int lastStepIndex = oneQueryStepToValues.size() - 1;

		boolean isLast = queryStepIndex.getAndIncrement() == lastStepIndex;
		String template;
		if (isLast) {
			template = "\\-- step %s";
		} else {
			template = "|\\- step %s";
		}
		explainLines.append(eol).append(template.formatted(stepAsString));
	}

	@Override
	public String toString() {
		return explainLines.toString();
	}
}
