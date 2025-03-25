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
package eu.solven.adhoc.measure.aggregation.comparable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.measure.sum.IAggregationCarrier;

public class TestRankAggregation {
	@Test
	public void rank2FromMax() {
		RankAggregation aggregation = RankAggregation.fromMax(2);

		// Aggregate null and item
		Object carrier3 = aggregation.aggregate(null, 3);

		{
			Assertions.assertThat(carrier3).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(null);
				});
			});
		}

		// Aggregate elements
		Object carrier13 = aggregation.aggregate(1, 3);
		Object carrier24 = aggregation.aggregate(2, 4);

		// Aggregate carriers together
		{
			Object carrier1234 = aggregation.aggregate(carrier13, carrier24);

			Assertions.assertThat(carrier1234).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(3);
				});
			});
		}

		// Aggregate element and carrier
		{
			Object carrier124 = aggregation.aggregate(1, carrier24);

			Assertions.assertThat(carrier124).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(2);
				});
			});
		}

		// Aggregate carrier and element
		{
			Object carrier241 = aggregation.aggregate(carrier24, 1);

			Assertions.assertThat(carrier241).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(2);
				});
			});
		}
	}

	@Test
	public void rank2FromMin() {
		RankAggregation aggregation = RankAggregation.fromMax(-2);

		// Aggregate null and item
		Object carrier3 = aggregation.aggregate(null, 3);

		{
			Assertions.assertThat(carrier3).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(null);
				});
			});
		}

		// Aggregate elements
		Object carrier13 = aggregation.aggregate(1, 3);
		Object carrier24 = aggregation.aggregate(2, 4);

		// Aggregate carriers together
		{
			Object carrier1234 = aggregation.aggregate(carrier13, carrier24);

			Assertions.assertThat(carrier1234).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(2);
				});
			});
		}

		// Aggregate element and carrier
		{
			Object carrier124 = aggregation.aggregate(1, carrier24);

			Assertions.assertThat(carrier124).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(2);
				});
			});
		}

		// Aggregate carrier and element
		{
			Object carrier241 = aggregation.aggregate(carrier24, 1);

			Assertions.assertThat(carrier241).isInstanceOfSatisfying(IAggregationCarrier.class, carrier -> {
				carrier.acceptValueReceiver(vc -> {
					Assertions.assertThat(vc).isEqualTo(2);
				});
			});
		}
	}

	@Test
	public void testWrap_thenAdd() {
		RankAggregation agg = RankAggregation.fromMax(2);
		Object o = IValueProvider.getValue(vr -> agg.wrap(123).add(234).acceptValueReceiver(vr));
		Assertions.assertThat(o).isEqualTo(123);
	}

	@Test
	public void testAggregate_thenAddCarrier() {
		RankAggregation agg = RankAggregation.fromMax(2);
		Object o = IValueProvider
				.getValue(vr -> agg.aggregate(123, 234).add(agg.aggregate(345, 567)).acceptValueReceiver(vr));
		Assertions.assertThat(o).isEqualTo(345);
	}

	@Test
	public void testAggregate_thenAddWrapped() {
		RankAggregation agg = RankAggregation.fromMax(2);
		Object o = IValueProvider.getValue(vr -> agg.aggregate(123, 234).add(agg.wrap(345)).acceptValueReceiver(vr));
		Assertions.assertThat(o).isEqualTo(234);
	}
}
