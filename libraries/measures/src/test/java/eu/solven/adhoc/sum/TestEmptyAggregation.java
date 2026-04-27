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
package eu.solven.adhoc.sum;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.EmptyAggregation;

public class TestEmptyAggregation {
	EmptyAggregation agg = new EmptyAggregation();

	@Test
	public void testNominal() {
		Assertions.assertThat(agg.aggregate(null, 123)).isEqualTo(null);
		Assertions.assertThat(agg.aggregateLongs(123, 234)).isEqualTo(0);
		Assertions.assertThat(agg.aggregateDoubles(12.34, 23.45)).isEqualTo(0D);
	}

	@Test
	public void testIsEmpty() {
		Assertions.assertThat(EmptyAggregation.isEmpty(EmptyAggregation.KEY)).isTrue();
		Assertions.assertThat(EmptyAggregation.isEmpty(EmptyAggregation.class.getName())).isTrue();
		Assertions.assertThat(EmptyAggregation.isEmpty(Aggregator.empty())).isTrue();
		Assertions.assertThat(EmptyAggregation.isEmpty(Set.of(Aggregator.empty()))).isTrue();

		Assertions.assertThat(EmptyAggregation.isEmpty(Set.of(Aggregator.sum("k1")))).isFalse();

		// Mixed empty + non-empty: no longer an error (used by Shiftor to materialize slices on top of a real
		// aggregator). The empty contributes only its slice-materialization effect, which is a no-op once the raw
		// row pass kicks in for the non-empty aggregator.
		Assertions.assertThat(EmptyAggregation.isEmpty(Set.of(Aggregator.empty(), Aggregator.sum("k1")))).isFalse();
	}
}
