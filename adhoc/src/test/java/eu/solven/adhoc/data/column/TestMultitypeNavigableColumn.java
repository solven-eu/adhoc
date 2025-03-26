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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestMultitypeNavigableColumn {
	IAggregation sum = new SumAggregation();

	MultitypeHashMergeableColumn<String> storage =
			MultitypeHashMergeableColumn.<String>builder().aggregation(sum).build();

	@Test
	public void testIntAndLong() {
		storage.merge("k1", 123);
		storage.merge("k1", 234L);

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(357L);
		});
	}

	@Test
	public void testIntAndDouble() {
		storage.merge("k1", 123);
		storage.merge("k1", 234.567D);

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(357.567D);
		});
	}

	@Test
	public void testIntAndString() {
		storage.merge("k1", 123);
		storage.merge("k1", "234");
		storage.merge("k1", 345);

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo(123 + "234" + 345);
		});
	}

	@Test
	public void testStringAndString() {
		storage.merge("k1", "123");
		storage.merge("k1", "234");

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isEqualTo("123234");
		});
	}

	@Test
	public void testClearKey() {
		storage.merge("k1", 123);
		storage.clearKey("k1");

		Assertions.assertThat(storage.size()).isEqualTo(0);
	}

	@Test
	public void testPutNull() {
		storage.merge("k1", 123);
		storage.append("k1", null);

		Assertions.assertThat(storage.size()).isEqualTo(1);
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

		storage.purgeAggregationCarriers();

		storage.onValue("k1", o -> {
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

		storage.purgeAggregationCarriers();

		storage.onValue("k1", o -> {
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

		storage.purgeAggregationCarriers();

		storage.onValue("k1", o -> {
			Assertions.assertThat(o).isInstanceOf(Integer.class).isEqualTo(1);
		});

		storage.onValue("k" + size, o -> {
			Assertions.assertThat(o).isInstanceOf(Integer.class).isEqualTo(size);
		});
	}

	@Test
	public void testCopyFromNotSorted() {
		IMultitypeColumnFastGet<String> notSorted = MultitypeHashColumn.<String>builder().build();

		List<String> randomKeys = new ArrayList<>(Arrays.asList("a", "b", "c", "d"));

		// Not seeded as we want the test to check various configurations through time
		Random r = new Random();
		long seed = r.nextLong();
		log.info("Seed={}", seed);
		r = new Random(seed);

		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onLong(123);
		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onDouble(23.45);
		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onCharsequence("someString");
		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onObject(LocalDate.now());

		IMultitypeColumnFastGet<String> sortedCopy = MultitypeNavigableColumn.copy(notSorted);

		List<String> slices = sortedCopy.stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(slices).hasSize(4).containsExactly("a", "b", "c", "d");
	}

	@Test
	public void testCopyFrom_carrierTurnsToNull() {
		IMultitypeColumnFastGet<String> notSorted = MultitypeHashColumn.<String>builder().build();

		RankAggregation agg = RankAggregation.fromMax(5);

		notSorted.append("a").onObject(agg.aggregate(123, 234));

		notSorted.purgeAggregationCarriers();

		IMultitypeColumnFastGet<String> sortedCopy = MultitypeNavigableColumn.copy(notSorted);

		List<String> slices = sortedCopy.stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(slices).isEmpty();
	}
}
