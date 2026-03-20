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
package eu.solven.adhoc.measure.lambda;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLambdaAggregation {

	@Test
	public void testAggregate_sum() {
		LambdaAggregation agg = new LambdaAggregation(
				Map.of(LambdaAggregation.K_LAMBDA, (LambdaAggregation.ILambdaAggregation) (l, r) -> {
					if (l == null)
						return r;
					if (r == null)
						return l;
					return ((Number) l).longValue() + ((Number) r).longValue();
				}));

		Assertions.assertThat(agg.aggregate(3L, 4L)).isEqualTo(7L);
	}

	@Test
	public void testAggregate_nullLeft() {
		LambdaAggregation agg = new LambdaAggregation(
				Map.of(LambdaAggregation.K_LAMBDA, (LambdaAggregation.ILambdaAggregation) (l, r) -> {
					if (l == null)
						return r;
					if (r == null)
						return l;
					return ((Number) l).longValue() + ((Number) r).longValue();
				}));

		Assertions.assertThat(agg.aggregate(null, 5L)).isEqualTo(5L);
	}

	@Test
	public void testConstruct_missingLambda() {
		Assertions.assertThatThrownBy(() -> new LambdaAggregation(Map.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
