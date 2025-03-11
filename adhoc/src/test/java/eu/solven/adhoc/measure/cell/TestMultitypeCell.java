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
package eu.solven.adhoc.measure.cell;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.storage.IValueReceiver;

public class TestMultitypeCell {
	@Test
	public void testCrossTypes_allTypes() {
		MultitypeCell cell = MultitypeCell.builder().aggregation(new SumAggregation()).build();

		cell.merge().onLong(123);
		cell.merge().onDouble(23.45);
		cell.merge().onObject("Arg");

		cell.reduce().acceptConsumer(new IValueReceiver() {

			@Override
			public void onLong(long v) {
				Assertions.fail("Should call the Object aggregate");
			}

			@Override
			public void onDouble(double v) {
				Assertions.fail("Should call the Object aggregate");
			}

			@Override
			public void onObject(Object v) {
				Assertions.assertThat(v).isEqualTo("Arg12323.45");
			}
		});
	}

	@Test
	public void testCrossTypes_longAndDouble() {
		MultitypeCell cell = MultitypeCell.builder().aggregation(new SumAggregation()).build();

		cell.merge().onLong(123);
		cell.merge().onDouble(23.45);

		cell.reduce().acceptConsumer(new IValueReceiver() {

			@Override
			public void onLong(long v) {
				Assertions.fail("Should call the Object aggregate");
			}

			@Override
			public void onDouble(double v) {
				Assertions.fail("Should call the Object aggregate");
			}

			@Override
			public void onObject(Object v) {
				Assertions.assertThat(v).isEqualTo(123 + 23.45D);
			}
		});
	}

	@Test
	public void testPrimitive_long() {
		MultitypeCell cell = MultitypeCell.builder().aggregation(new SumAggregation()).build();

		cell.merge().onLong(123);
		cell.merge().onLong(234);

		cell.reduce().acceptConsumer(new IValueReceiver() {

			@Override
			public void onLong(long v) {
				Assertions.assertThat(v).isEqualTo(123 + 234);
			}

			@Override
			public void onDouble(double v) {
				Assertions.fail("Should call the Long aggregate");
			}

			@Override
			public void onObject(Object v) {
				Assertions.fail("Should call the Long aggregate");
			}
		});
	}

	@Test
	public void testPrimitive_double() {
		MultitypeCell cell = MultitypeCell.builder().aggregation(new SumAggregation()).build();

		cell.merge().onDouble(12.34);
		cell.merge().onDouble(23.45);

		cell.reduce().acceptConsumer(new IValueReceiver() {

			@Override
			public void onLong(long v) {
				Assertions.fail("Should call the Double aggregate");
			}

			@Override
			public void onDouble(double v) {
				Assertions.assertThat(v).isEqualTo(12.34D + 23.45D);
			}

			@Override
			public void onObject(Object v) {
				Assertions.fail("Should call the Double aggregate");
			}
		});
	}
}
