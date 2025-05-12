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

import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestMultitypeNavigableColumn {
	MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

	@Test
	public void testCopyFromNotSorted() {
		IMultitypeColumnFastGet<String> notSorted = MultitypeHashColumn.<String>builder().build();

		List<String> randomKeys = new ArrayList<>(Arrays.asList("a", "b", "c"));

		// Not seeded as we want the test to check various configurations through time
		Random r = new SecureRandom();
		long seed = r.nextLong();
		// Print the (random) seed for reproduction if necessary
		log.info("Seed={}", seed);
		r = new Random(seed);

		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onLong(123);
		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onDouble(23.45);
		notSorted.append(randomKeys.remove(r.nextInt(randomKeys.size()))).onObject(LocalDate.now());

		IMultitypeColumnFastGet<String> sortedCopy = MultitypeNavigableColumn.copy(notSorted);

		List<String> slices = sortedCopy.stream().map(sm -> sm.getSlice()).toList();
		Assertions.assertThat(slices).hasSize(3).containsExactly("a", "b", "c");
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

	@Test
	public void testMerge() {
		MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

		Assertions.assertThatThrownBy(() -> column.merge(-1)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> column.merge(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> column.merge(1)).isInstanceOf(ArrayIndexOutOfBoundsException.class);

		column.append("foo").onLong(123);

		Assertions.assertThatThrownBy(() -> column.merge(-1)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
		Assertions.assertThatThrownBy(() -> column.merge(0)).isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> column.merge(1)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testWriteFailed() {
		MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

		// Append the key, but not the value
		column.append("foo");
		// Failure is triggered on following appender provisioning
		Assertions.assertThatThrownBy(() -> column.append("bar"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Missing push into IValueReceiver for key=foo");
	}

	@Test
	public void testMultipleWrites() {
		MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

		IValueReceiver fooAppender = column.append("foo");

		fooAppender.onLong(123);

		// BEWARE The issue is detected on next key provisioning, while we may want it to be reported earlier (on second
		// push).
		fooAppender.onLong(234);
		Assertions.assertThatThrownBy(() -> column.append("bar"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Multiple pushes into IValueReceiver for key=foo");
	}

	@Test
	public void testWriteOnLocked() {
		MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

		column.append("foo").onLong(123);

		column.doLock();

		Assertions.assertThatThrownBy(() -> column.append("bar"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("This is locked. Can not append key=bar");
	}

	@Test
	public void testToString() {
		MultitypeNavigableColumn<String> column = MultitypeNavigableColumn.<String>builder().build();

		LocalDate today = LocalDate.parse("2025-05-11");

		column.append("foo").onLong(123);
		column.append("bar").onObject(today);

		// This structure is sorted
		Assertions.assertThat(column.keyStream().toList()).containsExactly("bar", "foo");

		Assertions.assertThat(column.toString())
				.isEqualTo(
						"MultitypeNavigableColumn{size=2, #0=bar->2025-05-11(java.time.LocalDate), #1=foo->123(java.lang.Long)}");
	}

	@Test
	public void testNull() {
		column.append("k1").onObject(null);

		column.onValue("k1", o -> {
			Assertions.assertThat(o).isNull();
		});

		Assertions.assertThat(column.toString()).isEqualTo("MultitypeNavigableColumn{size=1, #0=k1->null(null)}");
	}

}
