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
package eu.solven.adhoc.beta.schema;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocQuery;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestTargetedAdhocQuery {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(TargetedAdhocQuery.class).verify();
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		TargetedAdhocQuery query = TargetedAdhocQuery.builder()
				.cube("someCube")
				.endpointId(UUID.randomUUID())
				.option(StandardQueryOptions.EXPLAIN)
				.query(AdhocQuery.builder().measure(Aggregator.countAsterisk()).groupByAlso("someColumn").build())
				.build();

		TestMapBasedTabularView.verifyJackson(TargetedAdhocQuery.class, query);
	}
}
