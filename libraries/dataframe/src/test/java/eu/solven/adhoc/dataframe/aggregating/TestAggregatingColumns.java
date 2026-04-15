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
package eu.solven.adhoc.dataframe.aggregating;

import java.lang.reflect.Field;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphStats;
import org.springframework.util.ReflectionUtils;

import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.UndictionarizedColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableIntColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.primitive.IValueProvider;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

public class TestAggregatingColumns {
	Aggregator a = Aggregator.sum("a");
	Aggregator b = Aggregator.sum("b");
	AggregatingColumns<String> aggregatingColumns = AggregatingColumns.<String>builder().build();

	@Test
	public void testUnknownKey() {
		aggregatingColumns.contribute("k", a).onLong(123);

		IMultitypeColumnFastGet<String> closedColumn =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), a);

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

		IMultitypeColumnFastGet<String> closed =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), Aggregator.sum("k"));
		Assertions.assertThat(closed).isInstanceOfSatisfying(MultitypeHashColumn.class, closed2 -> {
			Field field = ReflectionUtils.findField(MultitypeHashColumn.class, "sliceToL");
			field.setAccessible(true);
			Object2LongMap<?> sliceToValueL = (Object2LongMap<?>) ReflectionUtils.getField(field, closed2);
			Assertions.assertThat(sliceToValueL.getClass().getName())
					.hasToString("it.unimi.dsi.fastutil.objects.Object2LongMaps$EmptyMap");
		});
	}

	// ---- sortedPrefixLength tracking ----
	// `AggregatingColumns` differs from `AggregatingColumnsDistinct` in that re-visiting an existing slice via
	// `sliceToIndex.computeIfAbsent` MUST NOT advance the prefix even if the key happens to be "in order".

	@Test
	public void testSortedPrefix_emptyColumn_zero() {
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
	}

	@Test
	public void testSortedPrefix_singleNewSlice_one() {
		aggregatingColumns.contribute("k", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
	}

	@Test
	public void testSortedPrefix_singleNewSlice_misordered() {
		aggregatingColumns.contribute("1", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
		aggregatingColumns.contribute("2", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
		aggregatingColumns.contribute("0", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).hasSize(1).containsEntry("a", 2L);
		aggregatingColumns.contribute("3", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).hasSize(1).containsEntry("a", 2L);
	}

	@Test
	public void testSortedPrefix_singleNewSlice_misordered_newAggregatorAfterMisorder() {
		aggregatingColumns.contribute("1", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
		aggregatingColumns.contribute("2", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).isEmpty();
		aggregatingColumns.contribute("0", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).hasSize(1).containsEntry("a", 2L);
		aggregatingColumns.contribute("3", a).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).hasSize(1).containsEntry("a", 2L);

		aggregatingColumns.contribute("3", b).onLong(1L);
		Assertions.assertThat(aggregatingColumns.aggregatorToSortedLength).hasSize(1).containsEntry("a", 2L);

		Assertions.assertThat(aggregatingColumns.getNbSorted("a", aggregatingColumns.getColumn("a"))).isEqualTo(2);
		Assertions.assertThat(aggregatingColumns.getNbSorted("b", aggregatingColumns.getColumn("b"))).isEqualTo(0);
	}

	@Test
	public void testClose_has1() {
		aggregatingColumns.openSlice("row").contribute(a).onDouble(123D);

		Assertions.assertThat(aggregatingColumns.sliceToIndex)
				.isInstanceOfSatisfying(Object2IntOpenHashMap.class, openHashMap -> {
					// TODO Writing a single row allocated the maximum size: it feels bad
					Assertions.assertThat(GraphStats.parseInstance(openHashMap).totalSize())
							.isCloseTo(16777376L, Percentage.withPercentage(5D));
				});

		// Aggregation-time storage is still the generic MultitypeNavigableElseHashMergeableColumn (the default
		// mergeable column type). closeColumn() then copies it into a MultitypeNavigableIntColumn, which is what
		// the returned UndictionarizedColumn actually holds.
		Assertions.assertThat(aggregatingColumns.getColumn("a"))
				.isInstanceOf(MultitypeNavigableElseHashMergeableIntColumn.class);

		IMultitypeColumnFastGet<String> closed =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), a);
		Assertions.assertThat(closed).isInstanceOfSatisfying(UndictionarizedColumn.class, closed2 -> {
			Field columnField = ReflectionUtils.findField(UndictionarizedColumn.class, "column");
			columnField.setAccessible(true);
			Object inner = ReflectionUtils.getField(columnField, closed2);
			Assertions.assertThat(inner).isInstanceOf(MultitypeNavigableElseHashMergeableIntColumn.class);
		});
	}

	@Test
	public void testMisordered() {
		aggregatingColumns.openSlice("a1").contribute(a).onLong(2);
		aggregatingColumns.openSlice("a3").contribute(a).onLong(5);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(7);

		IMultitypeColumnFastGet<String> closedA =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), a);

		// ALL
		Assertions.assertThat(closedA.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(7L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).hasSize(3);

		// SORTED
		Assertions.assertThat(closedA.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).hasSize(2);

		// COMPLEMENT
		Assertions.assertThat(closedA.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(7L);
		}).hasSize(1);
	}

	// Re-encountering slices is legit in AggregatingColumns (while not in DistinctAggregatingColumns)
	@Test
	public void testMisordered_twoColumnsMisorderedDifferently_backOnExistingSlice() {
		// a1 -> a2 (not not on b) -> a1 -> a2 (even on b)
		aggregatingColumns.openSlice("a1").contribute(a).onLong(1);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(2);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(3);
		// aggregatingColumns.openSlice("a2").contribute(b).onLong(4);
		aggregatingColumns.openSlice("a1").contribute(a).onLong(5);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(6);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(7);
		aggregatingColumns.openSlice("a2").contribute(b).onLong(8);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L + 6L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(8L);
		}).hasSize(2);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L + 6L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(8L);
		}).hasSize(2);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).hasSize(0);
	}

	// Re-encountering slices is legit in AggregatingColumns (while not in DistinctAggregatingColumns)
	@Test
	public void testMisordered_twoColumnsMisorderedDifferently_backOnExistingSlice_firstOnSecondColumn() {
		// a1 -> a2 (not not on b) -> a1 -> a2 (even on b)
		aggregatingColumns.openSlice("a1").contribute(a).onLong(1);
		// aggregatingColumns.openSlice("a1").contribute(b).onLong(2);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(3);
		aggregatingColumns.openSlice("a2").contribute(b).onLong(4);
		aggregatingColumns.openSlice("a1").contribute(a).onLong(5);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(6);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(4L);
		}).hasSize(2);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(4L);
		}).hasSize(1);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).hasSize(1);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.ALL))).isEqualTo(6L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.ALL))).isEqualTo(4L);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB))).isEqualTo(4L);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(6L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
	}

	@Test
	public void testMisordered_twoColumnsMisorderedDifferently_backOnNewSlice() {
		// a1 -> a2 (not not on b) -> a1 -> a2 (even on b)
		aggregatingColumns.openSlice("a1").contribute(a).onLong(1);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(2);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(3);
		// aggregatingColumns.openSlice("a2").contribute(b).onLong(4);
		aggregatingColumns.openSlice("a0").contribute(a).onLong(5);
		aggregatingColumns.openSlice("a0").contribute(b).onLong(6);
		aggregatingColumns.openSlice("a3").contribute(a).onLong(7);
		aggregatingColumns.openSlice("a3").contribute(b).onLong(8);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a0");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(8L);
		}).hasSize(3);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(2L);
		}).hasSize(1);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a0");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(8L);
		}).hasSize(2);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.ALL))).isEqualTo(2L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.ALL))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a0", StreamStrategy.ALL))).isEqualTo(6L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.ALL))).isEqualTo(8L);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB))).isEqualTo(2L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a0", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.SORTED_SUB))).isNull();

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a0", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(6L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(8L);
	}

	@Test
	public void testMisordered_secondColumnIsOrdered() {
		// Slices are very misordered
		aggregatingColumns.openSlice("a3").contribute(a).onLong(1);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(2);
		aggregatingColumns.openSlice("a1").contribute(a).onLong(2);

		// columnB is perfectly ordered
		aggregatingColumns.openSlice("a1").contribute(b).onLong(4);
		aggregatingColumns.openSlice("a2").contribute(b).onLong(5);
		aggregatingColumns.openSlice("a3").contribute(b).onLong(6);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(4L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).hasSize(3);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).hasSize(0);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(4L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a2");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(6L);
		}).hasSize(3);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.ALL))).isEqualTo(4L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.ALL))).isEqualTo(5L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.ALL))).isEqualTo(6L);

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB))).isNull();
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.SORTED_SUB))).isNull();

		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a1", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(4L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a2", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(5L);
		Assertions.assertThat(IValueProvider.getValue(closedB.onValue("a3", StreamStrategy.SORTED_SUB_COMPLEMENT)))
				.isEqualTo(6L);
	}

	@Test
	public void testSparse_sorted() {
		aggregatingColumns.openSlice("a1").contribute(a).onLong(2);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(3);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(4);
		// aggregatingColumns.openSlice("a2").contribute(b).onLong(5);
		aggregatingColumns.openSlice("a3").contribute(a).onLong(6);
		aggregatingColumns.openSlice("a3").contribute(b).onLong(7);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(3L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(7L);
		}).hasSize(2);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(3L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(7L);
		}).hasSize(2);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).hasSize(0);
	}

	@Test
	public void testSparse_sortedComplement() {
		// a4 first to break ordering
		aggregatingColumns.openSlice("a4").contribute(a).onLong(2);
		aggregatingColumns.openSlice("a4").contribute(b).onLong(3);

		// a1, a2, a3 with a hole for one measure
		aggregatingColumns.openSlice("a1").contribute(a).onLong(4);
		aggregatingColumns.openSlice("a1").contribute(b).onLong(5);
		aggregatingColumns.openSlice("a2").contribute(a).onLong(6);
		// aggregatingColumns.openSlice("a2").contribute(b).onLong(7);
		aggregatingColumns.openSlice("a3").contribute(a).onLong(8);
		aggregatingColumns.openSlice("a3").contribute(b).onLong(9);

		IMultitypeColumnFastGet<String> closedB =
				aggregatingColumns.closeColumn(CubeQueryStep.builder().measure("m").build(), b);

		// ALL
		Assertions.assertThat(closedB.stream(StreamStrategy.ALL).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(9L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a4");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(3L);
		}).hasSize(3);

		// SORTED
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a4");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(3L);
		}).hasSize(1);

		// COMPLEMENT
		Assertions.assertThat(closedB.stream(StreamStrategy.SORTED_SUB_COMPLEMENT).toList()).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a1");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(5L);
		}).anySatisfy(s -> {
			Assertions.assertThat(s.getSlice()).isEqualTo("a3");
			Assertions.assertThat(IValueProvider.getValue(s.getValueProvider())).isEqualTo(9L);
		}).hasSize(2);
	}
}
