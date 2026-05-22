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
