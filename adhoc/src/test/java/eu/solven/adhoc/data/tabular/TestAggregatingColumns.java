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
package eu.solven.adhoc.data.tabular;

import java.lang.reflect.Field;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.UndictionarizedColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.column.navigable_else_hash.MultitypeNavigableElseHashMergeableColumn;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.primitive.IValueProvider;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

public class TestAggregatingColumns {
	Aggregator a = Aggregator.sum("c");
	AggregatingColumns<String> aggregatingColumns = AggregatingColumns.<String>builder().build();

	@Test
	public void testUnknownKey() {

		aggregatingColumns.contribute("k", a).onLong(123);

		IMultitypeColumnFastGet<String> closedColumn = aggregatingColumns.closeColumn(a);

		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("k"))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(closedColumn.onValue("unknownKey"))).isNull();

	}

	@Test
	public void testClose_empty() {
		Assertions.assertThat(aggregatingColumns.sliceToIndex)
				.isInstanceOfSatisfying(Object2IntOpenHashMap.class, openHashMap -> {
					Field field = ReflectionUtils.findField(Object2IntOpenHashMap.class, "value");
					field.setAccessible(true);
					int[] value = (int[]) ReflectionUtils.getField(field, openHashMap);
					Assertions.assertThat(value).hasSizeGreaterThan(1024);
				});

		IMultitypeColumnFastGet<String> closed = aggregatingColumns.closeColumn(Aggregator.sum("k"));
		Assertions.assertThat(closed).isInstanceOfSatisfying(MultitypeHashColumn.class, closed2 -> {
			Field field = ReflectionUtils.findField(MultitypeHashColumn.class, "sliceToValueL");
			field.setAccessible(true);
			Object2LongMap<?> sliceToValueL = (Object2LongMap<?>) ReflectionUtils.getField(field, closed2);
			Assertions.assertThat(sliceToValueL.getClass().getName())
					.hasToString("it.unimi.dsi.fastutil.objects.Object2LongMaps$EmptyMap");
		});
	}

	@Test
	public void testClose_has1() {
		aggregatingColumns.openSlice("row").contribute(a).onDouble(123D);

		Assertions.assertThat(aggregatingColumns.sliceToIndex)
				.isInstanceOfSatisfying(Object2IntOpenHashMap.class, openHashMap -> {
					Field field = ReflectionUtils.findField(Object2IntOpenHashMap.class, "value");
					field.setAccessible(true);
					int[] value = (int[]) ReflectionUtils.getField(field, openHashMap);
					Assertions.assertThat(value).hasSizeGreaterThan(1024);
				});

		IMultitypeColumnFastGet<String> closed = aggregatingColumns.closeColumn(a);
		Assertions.assertThat(closed).isInstanceOfSatisfying(UndictionarizedColumn.class, closed2 -> {
			Field field = ReflectionUtils.findField(UndictionarizedColumn.class, "column");
			field.setAccessible(true);
			MultitypeNavigableElseHashMergeableColumn<?> sliceToValueL =
					(MultitypeNavigableElseHashMergeableColumn<?>) ReflectionUtils.getField(field, closed2);
			Assertions.assertThat(sliceToValueL.getClass().getName())
					.hasToString(MultitypeNavigableElseHashMergeableColumn.class.getName());
		});
	}
}
