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
package eu.solven.adhoc.engine.tabular.inducer;

import java.util.Optional;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSortedSet;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Unit tests for {@link JavaStreamInducedEvaluator}, the universal Java-stream fallback path.
 *
 * @author Benoit Lacelle
 */
public class TestJavaStreamInducedEvaluator {

	AdhocFactories factories = AdhocFactories.builder().build();
	JavaStreamInducedEvaluator evaluator = new JavaStreamInducedEvaluator(factories);
	RowSliceFactory sliceFactory = RowSliceFactory.builder().build();

	/**
	 * Builds a simple {@link ICuboid} with two columns (country, id) and long values.
	 *
	 * <pre>
	 * country=FR id=1 -> 10
	 * country=FR id=2 -> 20
	 * country=DE id=3 -> 30
	 * country=DE id=4 -> 40
	 * </pre>
	 */
	ICuboid buildInducerValues() {
		IMultitypeColumnFastGet<ISlice> col = MultitypeHashColumn.<ISlice>builder().build();

		col.append(sliceFactory.newMapBuilder("country", "id").append("FR").append(1L).build().asSlice()).onLong(10L);
		col.append(sliceFactory.newMapBuilder("country", "id").append("FR").append(2L).build().asSlice()).onLong(20L);
		col.append(sliceFactory.newMapBuilder("country", "id").append("DE").append(3L).build().asSlice()).onLong(30L);
		col.append(sliceFactory.newMapBuilder("country", "id").append("DE").append(4L).build().asSlice()).onLong(40L);

		return Cuboid.builder().column("country").column("id").values(col).build();
	}

	@Test
	public void testSum_reduceGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		TableQueryStep inducer =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		TableQueryStep induced =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<ISlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		// JavaStream always returns a result
		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<ISlice> col = result.get();

		// Expect 2 groups: FR=30, DE=70
		Assertions.assertThat(col.size()).isEqualTo(2);

		ISlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		IValueProvider frValue = col.onValue(frSlice);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(frValue)).isEqualTo(30L);

		ISlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		IValueProvider deValue = col.onValue(deSlice);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(deValue)).isEqualTo(70L);
	}

	@Test
	public void testSum_withFilter() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		TableQueryStep inducer =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		TableQueryStep induced =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country")).build();

		// Only keep rows where country = "FR"
		ISliceFilter leftoverFilter = ColumnFilter.matchEq("country", "FR");

		Optional<IMultitypeMergeableColumn<ISlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, leftoverFilter, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<ISlice> col = result.get();

		// Only FR should appear (DE filtered out)
		Assertions.assertThat(col.size()).isEqualTo(1);

		ISlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(frSlice))).isEqualTo(30L);
	}

	@Test
	public void testMax_reduceGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(MaxAggregation.KEY).build();
		IAggregation aggregation = MaxAggregation.builder().build();

		TableQueryStep inducer =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		TableQueryStep induced =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<ISlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<ISlice> col = result.get();

		Assertions.assertThat(col.size()).isEqualTo(2);

		ISlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(frSlice))).isEqualTo(20L);

		ISlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(deSlice))).isEqualTo(40L);
	}

	/** When inducer and induced share the same groupBy columns, the sameColumns fast-path is taken. */
	@Test
	public void testSum_sameGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		TableQueryStep inducer =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		// induced has the same groupBy as inducer → sameColumns=true path
		TableQueryStep induced =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();

		Optional<IMultitypeMergeableColumn<ISlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<ISlice> col = result.get();

		// All 4 rows pass through unchanged
		Assertions.assertThat(col.size()).isEqualTo(4);

		ISlice fr1 = sliceFactory.newMapBuilder("country", "id").append("FR").append(1L).build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(fr1))).isEqualTo(10L);

		ISlice de4 = sliceFactory.newMapBuilder("country", "id").append("DE").append(4L).build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(de4))).isEqualTo(40L);
	}

	/** Empty input always returns an empty (but non-absent) column. */
	@Test
	public void testEmptyInput_returnsEmptyColumn() {
		ICuboid empty = Cuboid.empty();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		TableQueryStep inducer =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		TableQueryStep induced =
				TableQueryStep.builder().aggregator(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<ISlice>> result =
				evaluator.tryEvaluate(empty, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		// JavaStream always returns Optional.of(...), even for empty input
		Assertions.assertThat(result).isPresent();
		Assertions.assertThat(result.get().size()).isEqualTo(0);
	}

	/** A prefix of inducer columns preserves sort order — breakSorting should return false. */
	@Test
	public void testBreakSorting_prefixPreservesOrder() {
		// (a,b,c) → (a,b): induced is a prefix of inducer → no sort break
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(ImmutableSortedSet.of("a", "b", "c"),
				ImmutableSortedSet.of("a", "b"))).isFalse();
	}

	/** A non-prefix projection breaks sort order — breakSorting should return true. */
	@Test
	public void testBreakSorting_nonPrefixBreaksOrder() {
		// (a,b) → (b): "b" is not a prefix of "a,b" → sort is broken
		Assertions
				.assertThat(JavaStreamInducedEvaluator.breakSorting(ImmutableSortedSet.of("a", "b"),
						ImmutableSortedSet.of("b")))
				.isTrue();
	}

	/** Identical column sets are a trivially valid prefix — breakSorting returns false. */
	@Test
	public void testBreakSorting_sameColumns() {
		TreeSet<String> cols = new TreeSet<>(ImmutableSortedSet.of("a", "b"));
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(cols, cols)).isFalse();
	}

	@Test
	public void testBreakSorting() {
		ImmutableSortedSet<String> a = ImmutableSortedSet.of("a");
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(a, a)).isFalse();
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(a, ImmutableSortedSet.of())).isFalse();

		ImmutableSortedSet<String> ab = ImmutableSortedSet.of("a", "b");
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(a, ab)).isTrue();

		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(ab, a)).isFalse();
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(ab, ImmutableSortedSet.of())).isFalse();
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(ab, ImmutableSortedSet.of("b"))).isTrue();
		Assertions.assertThat(JavaStreamInducedEvaluator.breakSorting(ab, ImmutableSortedSet.of("a", "c"))).isTrue();
	}
}
