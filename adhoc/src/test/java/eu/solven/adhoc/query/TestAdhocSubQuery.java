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
package eu.solven.adhoc.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestAdhocSubQuery {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(AdhocSubQuery.class).verify();
	}

	// IHasMeasure may lead to StackOverFLow due to very lax default methods
	@Test
	public void testGetMeasures() {
		IAdhocQuery query = AdhocQuery.builder().measure("m").build();
		AdhocSubQuery subQuery =
				AdhocSubQuery.builder().subQuery(query).parentQueryId(AdhocQueryId.from(query)).build();

		Assertions.assertThat(subQuery.getMeasures()).hasSize(1).contains(ReferencedMeasure.ref("m"));
	}
}
