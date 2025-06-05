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
package eu.solven.adhoc.measure.aggregation;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation.CountAggregationCarrier;

public class TestCountAggregation {
	CountAggregation aggregation = new CountAggregation();

	@Test
	public void testSimple() {
		Assertions.assertThat(aggregation.aggregate((Object) null, null)).isEqualTo(null);

		Assertions.assertThat(aggregation.aggregate(123, null))
				.isEqualTo(CountAggregationCarrier.builder().count(1).build());
		Assertions.assertThat(aggregation.aggregate(null, "foo"))
				.isEqualTo(CountAggregationCarrier.builder().count(1).build());

		Assertions.assertThat(aggregation.aggregate(123, "foo"))
				.isEqualTo(CountAggregationCarrier.builder().count(2).build());

		Assertions.assertThat(aggregation.aggregate(null, CountAggregationCarrier.builder().count(2).build()))
				.isEqualTo(CountAggregationCarrier.builder().count(2).build());

		Assertions.assertThat(aggregation.aggregate(123, CountAggregationCarrier.builder().count(2).build()))
				.isEqualTo(CountAggregationCarrier.builder().count(3).build());

		Assertions
				.assertThat(aggregation.aggregate(CountAggregationCarrier.builder().count(3).build(),
						CountAggregationCarrier.builder().count(2).build()))
				.isEqualTo(CountAggregationCarrier.builder().count(5).build());
	}

	@Test
	public void testWrap() {
		Assertions.assertThat(aggregation.wrap(123)).isEqualTo(CountAggregationCarrier.builder().count(123).build());

		// With `count(*) FILTER ( c ='v')`, DuckDB may return 0
		Assertions.assertThat(aggregation.wrap(0)).isNull();

		Assertions.assertThatThrownBy(() -> aggregation.wrap(-123)).isInstanceOf(IllegalArgumentException.class);
	}
}
