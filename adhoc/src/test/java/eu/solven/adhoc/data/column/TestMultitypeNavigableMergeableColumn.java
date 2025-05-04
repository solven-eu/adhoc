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
package eu.solven.adhoc.data.column;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Collections2;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;

public class TestMultitypeNavigableMergeableColumn {
	IAggregation sum = new SumAggregation();

	MultitypeHashMergeableColumn<String> column =
			MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build();

	@Test
	public void testLongDoubleNull_Fuzzy() {
		Set<Consumer<MultitypeHashMergeableColumn<String>>> appenders = new LinkedHashSet<>();

		appenders.add(column -> column.merge("k1").onLong(123));
		appenders.add(column -> column.merge("k1").onLong(234));
		appenders.add(column -> column.merge("k1").onDouble(12.34D));
		appenders.add(column -> column.merge("k1").onDouble(23.45D));
		appenders.add(column -> column.merge("k1").onObject(null));

		Collections2.permutations(appenders).forEach(combination -> {
			column.clearKey("k1");

			combination.forEach(consumer -> consumer.accept(column));

			column.onValue("k1", o -> {
				Assertions.assertThat((Double) o)
						.isCloseTo(123 + 234 + 12.34D + 23.45D, Percentage.withPercentage(0.001));
			});
		});
	}

}
