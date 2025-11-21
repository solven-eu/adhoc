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

import java.util.concurrent.TimeUnit;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import eu.solven.adhoc.eventbus.AdhocLogEvent.AdhocLogEventBuilder;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps understanding a queryPlan for an {@link ICubeQuery}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class DagExplainerForPerfs extends DagExplainer {
	private static final String EOL = AdhocEventsFromGuavaEventBusToSfl4j.EOL;

	@Override
	protected AdhocLogEventBuilder openEventBuilder() {
		return super.openEventBuilder().performance(true);
	}

	@Override
	protected String additionalInfo(DagExplainerState dagState,
			CubeQueryStep step,
			String indentation,
			boolean isLast,
			boolean isReferenced) {
		if (isReferenced) {
			// This is a referenced step: no point in duplicating the performance information
			return "";
		}

		String prefix = indentation.replace('\\', ' ').replace('-', ' ');

		boolean hasNoChildren = dagState.getUnderlyingSteps(step).isEmpty();
		if (hasNoChildren) {
			prefix += "\\  ";
		} else {
			if ("/   ".equals(prefix)) {
				// This is the cost Info of the whole query
				// Should not add `| ` as it is already present with `/`, which is turned into `|`
				prefix = prefix.replace('/', '|') + "   ";
			} else {
				// Propagate the `|` to the children rows
				prefix += "|  ";
			}
		}

		SizeAndDuration cost = dagState.getDag().getStepToCost().get(step);
		if (cost == null) {
			return EOL + prefix + "No cost info";
		}

		return EOL + "%ssize=%s duration=%s".formatted(prefix,
				cost.getSize(),
				PepperLogHelper.humanDuration(cost.getDuration().toNanos(), TimeUnit.NANOSECONDS));
	}
}
