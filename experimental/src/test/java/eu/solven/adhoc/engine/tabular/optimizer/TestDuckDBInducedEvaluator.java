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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.Cuboid;
import eu.solven.adhoc.data.column.ICuboid;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.map.factory.RowSliceFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Unit tests for {@link DuckDBInducedEvaluator} that verify the DuckDB-backed vectorised path produces the same results
 * as the Java streaming path.
 *
 * @author Benoit Lacelle
 */
public class TestDuckDBInducedEvaluator {

	AdhocFactories factories = AdhocFactories.builder().build();
	FilterOptimizer filterOptimizer = FilterOptimizer.builder().build();
	DuckDBInducedEvaluator evaluator = new DuckDBInducedEvaluator(factories);
	RowSliceFactory sliceFactory = RowSliceFactory.builder().build();

	/**
	 * Builds a simple {@link ISliceToValue} with two columns (country, id) and long values.
	 *
	 * <pre>
	 * country=FR id=1 -> 10
	 * country=FR id=2 -> 20
	 * country=DE id=3 -> 30
	 * country=DE id=4 -> 40
	 * </pre>
	 */
	ICuboid buildInducerValues() {
		IMultitypeColumnFastGet<IAdhocSlice> col = MultitypeHashColumn.<IAdhocSlice>builder().build();

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

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<IAdhocSlice> col = result.get();

		// Expect 2 groups: FR=30, DE=70
		Assertions.assertThat(col.size()).isEqualTo(2);

		IAdhocSlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		IValueProvider frValue = col.onValue(frSlice);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(frValue)).isEqualTo(30L);

		IAdhocSlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		IValueProvider deValue = col.onValue(deSlice);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(deValue)).isEqualTo(70L);
	}

	@Test
	public void testSum_withFilter() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		// Only keep rows where country = "FR"
		ISliceFilter leftoverFilter = ColumnFilter.matchEq("country", "FR");

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, leftoverFilter, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<IAdhocSlice> col = result.get();

		// Only FR should appear (DE filtered out)
		Assertions.assertThat(col.size()).isEqualTo(1);

		IAdhocSlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(frSlice))).isEqualTo(30L);
	}

	@Test
	public void testMax_reduceGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(MaxAggregation.KEY).build();
		IAggregation aggregation = MaxAggregation.builder().build();

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<IAdhocSlice> col = result.get();

		Assertions.assertThat(col.size()).isEqualTo(2);

		IAdhocSlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(frSlice))).isEqualTo(20L);

		IAdhocSlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(deSlice))).isEqualTo(40L);
	}

	@Test
	public void testMin_reduceGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(MinAggregation.KEY).build();
		IAggregation aggregation = MinAggregation.builder().build();

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<IAdhocSlice> col = result.get();

		Assertions.assertThat(col.size()).isEqualTo(2);

		IAdhocSlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(frSlice))).isEqualTo(10L);

		IAdhocSlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getLong(col.onValue(deSlice))).isEqualTo(30L);
	}

	@Test
	public void testCount_reduceGroupBy() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(CountAggregation.KEY).build();
		IAggregation aggregation = new CountAggregation();

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		Assertions.assertThat(result).isPresent();
		IMultitypeMergeableColumn<IAdhocSlice> col = result.get();

		Assertions.assertThat(col.size()).isEqualTo(2);

		// Each country has 2 rows
		IAdhocSlice frSlice = sliceFactory.newMapBuilder("country").append("FR").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getObject(col.onValue(frSlice)))
				.isEqualTo(CountAggregation.CountAggregationCarrier.builder().count(2L).build());

		IAdhocSlice deSlice = sliceFactory.newMapBuilder("country").append("DE").build().asSlice();
		Assertions.assertThat(IValueProviderTestHelpers.getObject(col.onValue(deSlice)))
				.isEqualTo(CountAggregation.CountAggregationCarrier.builder().count(2L).build());
	}

	@Test
	public void testUnsupportedAggregation_returnsEmpty() {
		ICuboid inducerValues = buildInducerValues();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey("PRODUCT").build();
		IAggregation aggregation = factories.getOperatorFactory().makeAggregation(aggregator);

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(inducerValues, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		// PRODUCT is not supported by DuckDB path → must fall back (return empty)
		Assertions.assertThat(result).isEmpty();
	}

	@Test
	public void testEmptyInput_returnsEmpty() {
		ICuboid empty = Cuboid.empty();

		Aggregator aggregator = Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build();
		IAggregation aggregation = new SumAggregation();

		CubeQueryStep inducer =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country", "id")).build();
		CubeQueryStep induced =
				CubeQueryStep.builder().measure(aggregator).groupBy(GroupByColumns.named("country")).build();

		Optional<IMultitypeMergeableColumn<IAdhocSlice>> result =
				evaluator.tryEvaluate(empty, inducer, induced, ISliceFilter.MATCH_ALL, aggregation, aggregator);

		// Empty input → skip DuckDB path
		Assertions.assertThat(result).isEmpty();
	}
}
