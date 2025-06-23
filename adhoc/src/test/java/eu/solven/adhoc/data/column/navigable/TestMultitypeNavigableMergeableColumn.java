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
package eu.solven.adhoc.data.column.navigable;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Collections2;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;

public class TestMultitypeNavigableMergeableColumn {
	IAggregation sum = new SumAggregation();

	MultitypeNavigableMergeableColumn<String> column =
			MultitypeNavigableMergeableColumn.<String>builder().aggregation(sum).build();

	@Test
	public void testLongDoubleNull_Fuzzy() {
		Set<Consumer<IMultitypeMergeableColumn<String>>> appenders = new LinkedHashSet<>();

		appenders.add(column -> column.merge("k1").onLong(123));
		appenders.add(column -> column.merge("k1").onLong(234));
		appenders.add(column -> column.merge("k1").onDouble(12.34D));
		appenders.add(column -> column.merge("k1").onDouble(23.45D));
		appenders.add(column -> column.merge("k1").onObject(null));

		Collections2.permutations(appenders).forEach(combination -> {
			column.clearKey("k1");

			combination.forEach(consumer -> consumer.accept(column));

			column.onValue("k1").acceptReceiver(o -> {
				Assertions.assertThat((Double) o)
						.isCloseTo(123 + 234 + 12.34D + 23.45D, Percentage.withPercentage(0.001));
			});
		});
	}

	@Test
	public void testIntAndLong() {
		column.merge("k1", 123);
		column.merge("k1", 234L);

		column.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(357L);
		});
	}

	@Test
	public void testIntAndDouble() {
		column.merge("k1", 123);
		column.merge("k1", 234.567D);

		column.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(357.567D);
		});
	}

	@Test
	public void testIntAndString() {
		column.merge("k1", 123);
		column.merge("k1", "234");
		column.merge("k1", 345);

		column.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(123 + "234" + 345);
		});
	}

	@Test
	public void testStringAndString() {
		column.merge("k1", "123");
		column.merge("k1", "234");

		column.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo("123234");
		});
	}

	@Test
	public void testClearKey() {
		column.merge("k1", 123);
		column.clearKey("k1");

		Assertions.assertThat(column.size()).isEqualTo(0);
	}

	@Test
	public void testPutNull() {
		column.merge("k1", 123);
		column.append("k1", null);

		Assertions.assertThat(column.size()).isEqualTo(1);
	}

	@Test
	public void testPurgeAggregationCarriers() {
		MultitypeHashMergeableColumn<String> storage =
				MultitypeHashMergeableColumn.<String>builder().aggregation(RankAggregation.fromMax(2)).build();

		storage.merge("k1", 3);
		storage.merge("k1", 5);

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(RankAggregation.RankedElementsCarrier.class);
		});

		MultitypeHashColumn<String> purged = storage.purgeAggregationCarriers();

		purged.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(Integer.class).isEqualTo(3);
		});
	}

	@Test
	public void testPurgeAggregationCarriers_singleEntry() {
		MultitypeHashMergeableColumn<String> storage =
				MultitypeHashMergeableColumn.<String>builder().aggregation(RankAggregation.fromMax(2)).build();

		storage.merge("k1", 3);

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(RankAggregation.SingletonRankCarrier.class);
		});

		MultitypeHashColumn<String> purged = storage.purgeAggregationCarriers();

		purged.onValue("k1", o -> {
			Assertions.assertThat(o).isNull();
		});
	}

	// For consider a large problem, to pop issues around hashMap and non-linearities due to buckets
	@Test
	public void testPurgeAggregationCarriers_large() {
		MultitypeHashMergeableColumn<String> storage =
				MultitypeHashMergeableColumn.<String>builder().aggregation(RankAggregation.fromMax(2)).build();

		int size = 16 * 1024;

		IntStream.iterate(size, i -> i - 1).limit(size).forEach(i -> storage.merge("k" + i, i));
		IntStream.iterate(size, i -> i - 1).limit(size).forEach(i -> storage.merge("k" + i, 2 * i));

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(RankAggregation.RankedElementsCarrier.class);
		});

		MultitypeHashColumn<String> purged = storage.purgeAggregationCarriers();

		purged.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(Integer.class).isEqualTo(1);
		});

		purged.onValue("k" + size, o -> {
			Assertions.assertThat(o).isInstanceOf(Integer.class).isEqualTo(size);
		});
	}

}
