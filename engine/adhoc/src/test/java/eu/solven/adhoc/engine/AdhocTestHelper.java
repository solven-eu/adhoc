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
package eu.solven.adhoc.engine;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import lombok.experimental.UtilityClass;

/**
 * Commons patterns used in Adhoc unit-tests
 */
@UtilityClass
public class AdhocTestHelper {

	/**
	 * @return an EventBus with a registered {@link AdhocEventsFromGuavaEventBusToSfl4j}
	 */
	public static EventBus eventBus() {
		EventBus eventBus = new EventBus();

		// eventBus.register(new AdhocEventsFromGuavaEventBusToSfl4j());

		return eventBus;
	}
}
