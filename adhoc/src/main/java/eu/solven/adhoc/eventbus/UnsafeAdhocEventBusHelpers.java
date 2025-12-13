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

import java.util.List;

import com.google.common.eventbus.EventBus;

import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * Helps inter-operating EventBus an logging systems (e.g. to have a clear FQDN).
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Unclear of relevant")
@UtilityClass
public class UnsafeAdhocEventBusHelpers {
	private static final AdhocEventsFromGuavaEventBusToSfl4j TO_SLF4J = new AdhocEventsFromGuavaEventBusToSfl4j();

	/**
	 * Wraps an {@link IAdhocEventBus} so that all events ares forked into a plain SLF4J log, with proper FQDN
	 * management.
	 */
	@RequiredArgsConstructor
	public static class WrappingEventBusForSlf4jFQDN implements IAdhocEventBus {
		final IAdhocEventBus decorated;

		@Override
		public void post(Object event) {
			if (event instanceof IAdhocEvent logEvent) {
				logForkEventBus(decorated, logEvent.withFqdn(this.getClass().getName()));
			} else {
				decorated.post(event);
			}
		}
	}

	public static IAdhocEventBus safeWrapper(IAdhocEventBus eventBus) {
		return new WrappingEventBusForSlf4jFQDN(eventBus);
	}

	/**
	 * To be added to `LoggerContext#getFrameworkPackages()` in LogBack.
	 * 
	 * @param frameworkPackages
	 */
	public static void addToFrameworkPackages(List<String> frameworkPackages) {
		frameworkPackages.add("jdk.internal.reflect");
		frameworkPackages.add("java.lang.reflect");
		frameworkPackages.add("com.google.common.eventbus.Subscriber");
		frameworkPackages.add("com.google.common.util.concurrent.DirectExecutor");
		frameworkPackages.add("com.google.common.eventbus.Dispatcher");
		frameworkPackages.add("com.google.common.eventbus.EventBus");
		frameworkPackages.add(UnsafeAdhocEventBusHelpers.class.getName());
	}

	/**
	 * Utility methods to both submit the log to SLF4J and post it into an {@link EventBus}.
	 * 
	 * @param eventBus
	 * @param event
	 */
	public static void logForkEventBus(IAdhocEventBus eventBus, IAdhocEvent event) {
		if (event.getFqdn() == null) {
			event = event.withFqdn(UnsafeAdhocEventBusHelpers.class.getName());
		}

		// Will log to SLF4J
		TO_SLF4J.onAdhocEvent(event);

		// Transmit the event to an EventBus for decoupled processing
		eventBus.post(event);
	}
}
