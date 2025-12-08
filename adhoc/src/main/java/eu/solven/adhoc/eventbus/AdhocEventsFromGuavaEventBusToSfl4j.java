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
import java.util.regex.Pattern;

import org.slf4j.spi.LocationAwareLogger;

import com.google.common.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

/**
 * This logs main steps of the query-engine. It is typically activated by calling `#CubeQueryBuilder.debug()`.
 *
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class AdhocEventsFromGuavaEventBusToSfl4j implements IAdhocEventsListener {
	public static final String EOL = System.lineSeparator();
	private static final Pattern EOL_PATTERN = Pattern.compile(System.lineSeparator());

	// This must not have `@Subscribe`, else events would be processed multiple times
	// This is useful when the EventBus is not Guava
	public void onAdhocEvent(IAdhocEvent event) {
		if (event instanceof QueryStepIsCompleted queryStepIsCompleted) {
			onQueryStepIsCompleted(queryStepIsCompleted);
		} else if (event instanceof AdhocQueryPhaseIsCompleted queryPhaseIsCompleted) {
			onAdhocQueryPhaseIsCompleted(queryPhaseIsCompleted);
		} else if (event instanceof QueryStepIsEvaluating queryStepIsEvaluating) {
			onQueryStepIsEvaluating(queryStepIsEvaluating);
		} else if (event instanceof TableStepIsCompleted queryPhaseIsCompleted) {
			onTableStepIsCompleted(queryPhaseIsCompleted);
		} else if (event instanceof TableStepIsEvaluating queryStepIsEvaluating) {
			onTableStepIsEvaluating(queryStepIsEvaluating);
		} else if (event instanceof AdhocLogEvent logEvent) {
			onAdhocLogEvent(logEvent);
		} else if (event instanceof QueryLifecycleEvent lifecycleEvent) {
			onQueryLifecycleEvent(lifecycleEvent);
		} else {
			log.warn("Not managed properly: {}", event);
		}
	}

	/**
	 * An {@link eu.solven.adhoc.query.cube.CubeQuery} is resolved through a DAG of
	 * {@link eu.solven.adhoc.engine.step.CubeQueryStep}. This will log when an
	 * {@link eu.solven.adhoc.engine.step.CubeQueryStep} is completed.
	 * 
	 * @param event
	 */
	@Subscribe
	@Override
	public void onQueryStepIsCompleted(QueryStepIsCompleted event) {
		log.debug("size={} for queryStep={} on completed (source={})",
				event.getNbCells(),
				event.getQuerystep(),
				event.getSource());
	}

	@Subscribe
	@Override
	public void onAdhocQueryPhaseIsCompleted(AdhocQueryPhaseIsCompleted event) {
		log.debug("query phase={} is completed (source={})", event.getPhase(), event.getSource());
	}

	@Subscribe
	@Override
	public void onQueryStepIsEvaluating(QueryStepIsEvaluating event) {
		log.debug("queryStep={} is evaluating (source={})", event.getQueryStep(), event.getSource());
	}

	@Subscribe
	@Override
	public void onTableStepIsEvaluating(TableStepIsEvaluating event) {
		log.debug("tableStep={} is evaluating (source={})", event.getTableQuery(), event.getSource());
	}

	@Subscribe
	@Override
	public void onTableStepIsCompleted(TableStepIsCompleted event) {
		log.debug("tableStep={} is completed (source={})", event.getTableQuery(), event.getSource());
	}

	@Subscribe
	@Override
	public void onQueryLifecycleEvent(QueryLifecycleEvent event) {
		// Log first the tags, as the queryId is very redundant
		log.debug("queryLifecycleEvent tags={} queryId={}", event.getTags(), event.getQuery().getQueryId());
	}

	@Subscribe
	@Override
	public void onAdhocLogEvent(AdhocLogEvent event) {
		BiConsumer<String, Object[]> logMethod;

		if (log instanceof LocationAwareLogger lAwareLogger) {
			int logLevel;
			if (event.isWarn()) {
				logLevel = LocationAwareLogger.WARN_INT;
			} else {
				logLevel = LocationAwareLogger.INFO_INT;
			}

			String fqdn = this.getClass().getName();

			// https://stackoverflow.com/questions/3491744/wrapping-the-slf4j-api
			logMethod = (template, parameters) -> lAwareLogger.log(null, fqdn, logLevel, template, parameters, null);
		} else {
			if (event.isWarn()) {
				logMethod = log::warn;
			} else {
				logMethod = log::info;
			}
		}

		printLogEvent(event, logMethod);
	}

	/**
	 * 
	 * @param event
	 * @param logMethod
	 *            Will print, given a message template as key, and a array with values as value.
	 */
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected void printLogEvent(AdhocLogEvent event, BiConsumer<String, Object[]> logMethod) {
		if (event.isExplain() && event.getMessage().contains(EOL)) {
			Object[] arguments = { event.isDebug() ? "[DEBUG]" : "",
					event.isExplain() ? "[EXPLAIN]" : "",
					event.getMessage(),
					event.getSource() };
			// In EXPLAIN, we want rows to be well aligned, as we print some sort of ascii-graph
			EOL_PATTERN.splitAsStream(event.getMessage()).forEach(messageRow -> {
				arguments[2] = messageRow;

				// BEWARE Do not use `Stream.of` else the logger would refer a different class, and break the alignment
				logMethod.accept("{}{} {} (source={})", arguments);
			});
		} else {
			Object[] arguments = { event.isDebug() ? "[DEBUG]" : "",
					event.isExplain() ? "[EXPLAIN]" : "",
					event.getMessage(),
					event.getSource() };
			logMethod.accept("{}{} {} (source={})", arguments);
		}
	}

}
