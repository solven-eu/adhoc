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
package eu.solven.adhoc.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;

public class TestAggregator {
	@Test
	public void testWithoutColumnName() {
		Aggregator aggregator = Aggregator.builder().name("someName").build();

		Assertions.assertThat(aggregator.getName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getColumnName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getAggregationKey()).isEqualTo(SumAggregation.KEY);
	}

	@Test
	public void testWithColumnName() {
		Aggregator aggregator = Aggregator.builder().name("someName").columnName("otherColumnName").build();

		Assertions.assertThat(aggregator.getName()).isEqualTo("someName");
		Assertions.assertThat(aggregator.getColumnName()).isEqualTo("otherColumnName");
		Assertions.assertThat(aggregator.getAggregationKey()).isEqualTo(SumAggregation.KEY);
	}

	@Test
	public void testEdit() {
		Aggregator aggregator = Aggregator.builder()
				.name("someName")
				.columnName("otherColumnName")
				.aggregationKey("someKey")
				.aggregationOption("someOptionKey", "someOptionValue")
				.tag("someTag")
				.build();

		Aggregator copy = Aggregator.edit(aggregator).build();

		Assertions.assertThat(copy).isEqualTo(aggregator);
	}

	@Test
	public void testColumnName_Edit() {
		Aggregator aggregator = Aggregator.builder().name("someName").build();

		// with `.edit`
		{
			Aggregator copy = Aggregator.edit(aggregator).name("otherName").build();

			// The columnName has to be fixed when building the initial aggregator
			Assertions.assertThat(copy.getColumnName()).isEqualTo(aggregator.getName());
		}

		// with `.toBuilder`
		{
			Aggregator copy = aggregator.toBuilder().name("otherName").build();

			// The columnName has to be fixed when building the initial aggregator
			Assertions.assertThat(copy.getColumnName()).isEqualTo(aggregator.getName());
		}
	}

	@Test
	public void testToString() {
		Assertions.assertThat(Aggregator.builder().name("someName").build()).hasToString("someName:SUM(someName)");
		Assertions.assertThat(Aggregator.builder()
				.name("someName")
				.columnName("otherColumnName")
				.aggregationKey("someKey")
				.aggregationOption("someOptionKey", "someOptionValue")
				.tag("someTag")
				.build()).hasToString("someName:someKey(otherColumnName){someOptionKey=someOptionValue}");
	}
}
