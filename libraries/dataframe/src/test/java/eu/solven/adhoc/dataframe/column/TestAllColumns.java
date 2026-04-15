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
package eu.solven.adhoc.dataframe.column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashIntColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableIntColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.primitive.IValueFunction;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;

public class TestAllColumns {
	List<IMultitypeColumnFastGet<Integer>> columns = ImmutableList.<IMultitypeColumnFastGet<Integer>>builder()
			.add(MultitypeHashColumn.<Integer>builder().build())
			.add(MultitypeHashIntColumn.builder().build())

			.add(MultitypeNavigableColumn.<Integer>builder().build())
			.add(MultitypeNavigableIntColumn.builder().build())

			.add(MultitypeNavigableElseHashColumn.<Integer>builder().build())
			.add(MultitypeNavigableElseHashIntColumn.builder().build())
			.build();

	IAggregation sum = new SumAggregation();

	List<IMultitypeMergeableColumn<Integer>> mergeableColumns =
			ImmutableList.<IMultitypeMergeableColumn<Integer>>builder()
					.add(MultitypeHashMergeableColumn.<Integer>builder().aggregation(sum).build())
					.add(MultitypeHashMergeableIntColumn.builder().aggregation(sum).build())

					.add(MultitypeNavigableMergeableColumn.<Integer>builder().aggregation(sum).build())
					.add(MultitypeNavigableMergeableIntColumn.builder().aggregation(sum).build())

					.add(MultitypeNavigableElseHashMergeableColumn.<Integer>builder()
							.aggregation(sum)
							.navigable(MultitypeNavigableMergeableColumn.<Integer>builder().aggregation(sum).build())
							.hash(MultitypeHashMergeableColumn.<Integer>builder().aggregation(sum).build())
							.build())
					.add(MultitypeNavigableElseHashMergeableIntColumn.builder()
							.aggregation(sum)
							.navigable(MultitypeNavigableMergeableIntColumn.builder().aggregation(sum).build())
							.hash(MultitypeHashMergeableIntColumn.builder().aggregation(sum).build())
							.build())
					.build();

	List<IMultitypeColumnFastGet<Integer>> allColumns() {
		return ImmutableList.<IMultitypeColumnFastGet<Integer>>builder()
				.addAll(columns)
				.addAll(mergeableColumns)
				.build();
	}

	@Test
	public void testNormal() {
		columns.forEach(column -> {
			Assertions.assertThat(column.isEmpty()).isTrue();

			column.append(0).onLong(123);
			Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(0))).isEqualTo(123L);

			column.append(1).onObject(null);
			column.append(2).onDouble(12.34);
			column.append(3).onObject("foo");

			Assertions.assertThat(column.keyStream().toList()).containsExactly(0, 2, 3);

			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(0))).isEqualTo(123L);
			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(2))).isEqualTo(12.34D);
			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(3))).isEqualTo("foo");

			List<Object> intoList = new ArrayList<>();
			column.scan(s -> o -> intoList.add(Map.entry(s, o)));
			Assertions.assertThat(intoList).hasSize(3);
		});
	}

	@Test
	public void testMergeable() {
		mergeableColumns.forEach(column -> {
			Assertions.assertThat(column.isEmpty()).isTrue();

			column.append(0).onLong(123);
			Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(0))).isEqualTo(123L);
			column.merge(0).onLong(234);
			Assertions.assertThat(IValueProviderTestHelpers.getLong(column.onValue(0))).isEqualTo(123L + 234L);

			column.append(1).onObject(null);
			column.append(2).onDouble(12.34);
			column.append(3).onObject("foo");

			Assertions.assertThat(column.keyStream().toList()).containsExactly(0, 2, 3);

			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(0))).isEqualTo(123L + 234L);
			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(2))).isEqualTo(12.34D);
			Assertions.assertThat(IValueProviderTestHelpers.getObject(column.onValue(3))).isEqualTo("foo");
		});
	}

	@Test
	public void limit_ordered() {
		allColumns().forEach(column -> {
			column.append(0).onLong(123);
			column.append(1).onLong(234);

			if (column.getClass().getName().contains("Navigable")) {
				Assertions.assertThat(column.limit(1).map(s -> s.getSlice()).toList()).containsExactly(0);
				Assertions.assertThat(column.limit(2).map(s -> s.getSlice()).toList()).containsExactly(0, 1);
				Assertions.assertThat(column.skip(1).map(s -> s.getSlice()).toList()).containsExactly(1);
				Assertions.assertThat(column.skip(2).map(s -> s.getSlice()).toList()).containsExactly();
			}
		});
	}

	@Test
	public void test_toString() {
		allColumns().forEach(column -> {
			column.append(0).onLong(123);
			column.append(1).onLong(234);

			Assertions.assertThat(column.toString()).contains("#0-0=123(java.lang.Long), #1-1=234(java.lang.Long)");
		});
	}

	@Test
	public void testColumnValuerConverter() {
		IValueFunction<Long> f = i -> ((Long) i + 1) * 3;

		allColumns().forEach(column -> {
			column.append(0).onLong(123);
			column.append(1).onLong(234);

			Assertions.assertThat(column.stream(i -> f).toList()).containsExactly((123L + 1) * 3, (234L + 1) * 3);
		});
	}

	@Test
	public void purgeAggregationCarrier() {
		allColumns().forEach(column -> {
			column.append(0).onLong(123);
			column.append(1).onLong(234);

			Assertions.assertThat(column.purgeAggregationCarriers().keyStream().toList()).containsExactly(0, 1);
		});
	}
}
