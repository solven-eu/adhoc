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
package eu.solven.adhoc.pivotable.query;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;

public class TestPivotableQueryHandler {
	PivotableAdhocSchemaRegistry schemaRegistry = null;
	AdhocCubesRegistry cubesRegistry = null;
	PivotableQueryHandler handler = new PivotableQueryHandler(schemaRegistry, cubesRegistry);

	@Test
	public void testBackoff() {
		UUID queryId = UUID.randomUUID();

		Optional<Duration> first = handler.getRetryIn(queryId, AsynchronousStatus.RUNNING);
		Assertions.assertThat(first).isPresent().contains(Duration.ofMillis(100));

		Optional<Duration> second = handler.getRetryIn(queryId, AsynchronousStatus.RUNNING);
		Assertions.assertThat(second).isPresent().contains(Duration.ofMillis(110));

		Optional<Duration> third = handler.getRetryIn(queryId, AsynchronousStatus.RUNNING);
		Assertions.assertThat(third).isPresent().contains(Duration.ofMillis(121));
	}
}
