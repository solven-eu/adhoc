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
package eu.solven.adhoc.table.duckdb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.tabular.optimizer.CubeWrapperEditor;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.model.column.FunctionCalculatedColumn;
import eu.solven.adhoc.model.column.TableExpressionColumn;
import eu.solven.adhoc.model.measure.Partitionor;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

public class TestDagTableQuery_DuckDb_ExpressionColumn extends ATestDagDuckDb implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	TableQuery qK1 = TableQuery.builder().aggregators(Set.of(k1Sum)).build();

	/**
	 * Shared schema for every test in this class: {@code word, color, k1}. Tests that don't care about {@code color}
	 * simply ignore it.
	 */
	@BeforeEach
	public void feedTable() {
		dsl.createTableIfNotExists(tableName)
				.column("word", SQLDataType.VARCHAR)
				.column("color", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.INTEGER)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("word"), DSL.field("color"), DSL.field("k1"))
				.values("azerty", "blue", 123)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("word"), DSL.field("color"), DSL.field("k1"))
				.values("qwerty", "red", 234)
				.execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testWholeQuery() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.groupBy(GroupByColumns.of(TableExpressionColumn.builder().name("first_letter").sql("word[1]").build()))
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("first_letter", "a"), Map.of(k1Sum.getName(), 123L))
				.containsEntry(Map.of("first_letter", "q"), Map.of(k1Sum.getName(), 234L));
	}

	/**
	 * Reproducer: same calculated column ({@code first_letter = word[1]}), but with an additional filter on the
	 * calculated column. The filter on a column that does not exist in the underlying table cannot be transcoded into
	 * SQL — it becomes a leftover. The recent fix in {@code JooqTableQueryFactory.makeGroupingFields} adds the
	 * leftover-filter column to GROUP BY at SQL level so the post-fetch filter has real per-row values to match
	 * against; that propagates an extra column on every record returned by the JOOQ table wrapper.
	 *
	 * <p>
	 * The downstream stage {@code TabularRecordStreamReducer} hasn't been taught about those extra columns: its
	 * {@code columnsToMarker} is built from the user's declared groupBys only, so a record whose key set includes the
	 * leftover-filter column matches no marker and the lookup at line ~145 throws {@code NullPointerException} with
	 * "each scanned record must match a registered groupBy".
	 *
	 * <p>
	 * The expected (post-fix) behaviour is that the filter is honoured (only the row whose first letter matches
	 * survives) and the response contains a single slice keyed by {@code first_letter}.
	 */
	@Test
	public void testWholeQuery_filterOnCalculatedColumn() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.groupBy(GroupByColumns.of(TableExpressionColumn.builder().name("first_letter").sql("word[1]").build()))
				.andFilter("first_letter", "a")
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("first_letter", "a"), Map.of(k1Sum.getName(), 123L));
	}

	/**
	 * Reproducer for the {@code TabularRecordStreamReducer} fallout from the recent JooqTableQueryFactory leftover-
	 * filter fix. The scenario:
	 *
	 * <ul>
	 * <li>The cube has a Java-side {@code FunctionCalculatedColumn} {@code first_letter} computed from {@code word}.
	 * <li>The query groups by {@code word} (a real table column) and filters on {@code first_letter} (the calculated
	 * one). The filter cannot be transcoded into SQL — the computation lives in Java.
	 * <li>The filter therefore becomes a leftover. The recently-fixed {@code JooqTableQueryFactory.makeGroupingFields}
	 * adds the leftover-filter column ({@code word} here, since that's the input to {@code first_letter}) to GROUP BY
	 * at SQL level so the post-fetch filter has real per-row values to match against.
	 * <li>Records returned by the JOOQ table wrapper now carry an extra column ({@code word}) on top of the user's
	 * declared groupBy ({@code word} too, in this case — actually let's pick a setup where the leftover column is NOT
	 * in the declared groupBy, so the extra-column issue surfaces).
	 * </ul>
	 *
	 * <p>
	 * The expected (post-fix) behaviour is that the filter is honoured and the response contains slices for the
	 * matching word(s) only. Today, {@code TabularRecordStreamReducer.makeGroupingSetAnalyzer}'s
	 * {@code columnsToMarker} is built from the user's declared groupBys and does not know about the extra column — the
	 * lookup throws {@code NullPointerException} with "each scanned record must match a registered groupBy".
	 */
	@Test
	public void testWholeQuery_filterOnFunctionCalculatedColumn() {
		// Wire `first_letter` as a Java-side calculated column on top of `word`. Filtering on it cannot be
		// transcoded into SQL.
		ICubeWrapper cube = CubeWrapperEditor.edit(cube())
				.addCalculatedColumn(FunctionCalculatedColumn.builder().name("first_letter").recordToCoordinate(r -> {
					Object word = r.getGroupBy("word");
					if (word == null) {
						return null;
					} else {
						return word.toString().substring(0, 1);
					}
				}).build())
				.build();

		ITabularView result = cube.execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				// Grand total — NO groupBy. The filter is on `first_letter`, which is calculated from `word`,
				// so `word` becomes a leftover-filter column. The recently-fixed JooqTableQueryFactory adds
				// `word` to GROUP BY at SQL level so the post-fetch filter can read its value. The records
				// returned therefore carry a `word` column that the user did NOT request — the
				// TabularRecordStreamReducer's columnsToMarker (built from the user's groupBys, which is empty
				// here) does not have an entry whose keyset is `{"word"}`, and the lookup throws.
				.andFilter("first_letter", "a")
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Grand-total slice with only the row whose first letter is 'a' (123).
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 123L));
	}

	/**
	 * Forces a multi-groupBy {@code TableQueryV4} at the table layer by combining a flat aggregator ({@code k1.SUM} on
	 * grand total) with a {@code Partitionor} that widens the groupBy to {@code color} ({@code k1.SUM.maxByColor}). The
	 * cube optimizer emits two distinct groupBys for the same SQL — one with the empty key set, one with
	 * {@code {color}} — and {@code TabularRecordStreamReducer} therefore takes the multi-groupBy {@code else} branch
	 * with a {@code columnsToMarker} keyed on those two key sets.
	 *
	 * <p>
	 * On top of that, the query carries a filter on a Java-side {@code FunctionCalculatedColumn} {@code first_letter}
	 * (computed from {@code word}); the filter cannot be transcoded into SQL and becomes a leftover. The recently-fixed
	 * {@code JooqTableQueryFactory.makeGroupingFields} adds the leftover-filter column ({@code word}) to GROUP BY at
	 * SQL level so the post-fetch filter has real per-row values to match against. Records returned therefore carry an
	 * extra {@code word} column on top of the user's groupBy keys — the {@code columnsToMarker} lookup at line ~145 of
	 * {@code TabularRecordStreamReducer} sees keysets like {@code {word}} or {@code {color, word}} and finds no entry.
	 * Today this throws {@code NullPointerException} with "each scanned record must match a registered groupBy".
	 *
	 * <p>
	 * Expected post-fix: the leftover column is treated as a known-but-not-grouping-key extra; the reducer trims it
	 * before the lookup, the marker is found, and the response carries both measures' grand-total values for the
	 * surviving rows.
	 */
	@Test
	public void testMultiGroupBy_partitionor_filterOnFunctionCalculatedColumn() {
		List<String> explainMessages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBusGuava());

		forest.addMeasure(Partitionor.builder()
				.name("k1.SUM.maxByColor")
				.underlying(k1Sum.getName())
				.aggregationKey(MaxAggregation.KEY)
				.groupBy(GroupByColumns.named("color"))
				.build());
		// Second partitionor on `word` — neither (color) nor (word) induces the other, so the optimizer
		// cannot collapse the two underlying steps into a single SQL `GROUP BY`.
		forest.addMeasure(Partitionor.builder()
				.name("k1.SUM.minByWord")
				.underlying(k1Sum.getName())
				.aggregationKey(MinAggregation.KEY)
				.groupBy(GroupByColumns.named("word"))
				.build());

		ICubeWrapper cube = CubeWrapperEditor.edit(cube())
				.addCalculatedColumn(FunctionCalculatedColumn.builder().name("first_letter").recordToCoordinate(r -> {
					Object word = r.getGroupBy("word");
					if (word == null) {
						return null;
					} else {
						return word.toString().substring(0, 1);
					}
				}).build())
				.build();

		ITabularView result = cube.execute(CubeQuery.builder()
				.measure(k1Sum.getName(), "k1.SUM.maxByColor", "k1.SUM.minByWord")
				.andFilter("first_letter", "a")
				.explain(true)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Expected (post-fix): three measures, each at grand total, each at 123L (only `azerty` survives the
		// `first_letter == 'a'` filter; max-by-color of [123]=123; min-by-word of [123]=123).
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.as("explain trace:%n%s", String.join("\n", explainMessages))
				.hasSize(1)
				.containsEntry(Map.of(),
						Map.of(k1Sum.getName(), 123L, "k1.SUM.maxByColor", 123L, "k1.SUM.minByWord", 123L));
	}

	// ── Edge cases that any post-fetch column-cleanup fix must NOT break ─────────────────────────────────

	/**
	 * Single-groupBy on a Java-side calculated column. The reducer's UniqueGroupingSetAnalyzer (single-groupBy path)
	 * trims records to the user's groupBy keyset, so the calculated column must still be present in the record's slice
	 * for the result to be keyed by it. Any cleanup pass that drops {@code first_letter} from records will make this
	 * test return results keyed by {@code grandTotal} instead of by {@code first_letter}.
	 *
	 * <p>
	 * This case is the SUCCESS counterpart to the multi-groupBy failure: the same calculated-column machinery, but here
	 * the reducer sees a single groupBy that includes the calculated column, so the existing single-groupBy trim does
	 * the right thing.
	 */
	@Test
	public void testSingleGroupBy_filterAndGroupByOnFunctionCalculatedColumn() {
		ICubeWrapper cube = CubeWrapperEditor.edit(cube())
				.addCalculatedColumn(FunctionCalculatedColumn.builder().name("first_letter").recordToCoordinate(r -> {
					Object word = r.getGroupBy("word");
					if (word == null) {
						return null;
					} else {
						return word.toString().substring(0, 1);
					}
				}).build())
				.build();

		ITabularView result = cube.execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.groupByAlso("first_letter")
				.andFilter("first_letter", "a")
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Result must be keyed by `first_letter`, not by the underlying `word`.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("first_letter", "a"), Map.of(k1Sum.getName(), 123L));
	}

	/**
	 * Single-groupBy on a calculated column with NO filter. The simplest legitimate use of a calculated column —
	 * results keyed by it. Mirrors {@code TestDagCubeQuery_CalculatedColumn.test_groupBy_definitionInQuery} but on the
	 * JOOQ path. Any cleanup pass that strips calculated columns post-fetch breaks this.
	 */
	@Test
	public void testSingleGroupBy_calculatedColumn_noFilter() {
		ICubeWrapper cube = CubeWrapperEditor.edit(cube())
				.addCalculatedColumn(FunctionCalculatedColumn.builder().name("first_letter").recordToCoordinate(r -> {
					Object word = r.getGroupBy("word");
					if (word == null) {
						return null;
					} else {
						return word.toString().substring(0, 1);
					}
				}).build())
				.build();

		ITabularView result =
				cube.execute(CubeQuery.builder().measure(k1Sum.getName()).groupByAlso("first_letter").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("first_letter", "a"), Map.of(k1Sum.getName(), 123L))
				.containsEntry(Map.of("first_letter", "q"), Map.of(k1Sum.getName(), 234L));
	}

	/**
	 * Multi-groupBy via two Partitionors with NO calculated-column filter. There is no post-fetch column hoisting here
	 * (no leftover columns); the two Partitionors' grouping sets ({@code (color)} and {@code (word)}) are
	 * non-induceable, so the optimizer materializes a multi-grouping-set SQL anyway. The reducer's keyset-matching path
	 * (the same {@code else} branch as the failing reproducer) is exercised, and must stay green.
	 *
	 * <p>
	 * This pins that the multi-grouping-set reducer path itself works correctly; the failure in
	 * {@link #testMultiGroupBy_partitionor_filterOnFunctionCalculatedColumn} is specifically about the
	 * <i>interaction</i> between multi-grouping-sets and calculated-column filtering.
	 */
	@Test
	public void testMultiGroupBy_partitionor_noCalculatedColumn() {
		forest.addMeasure(Partitionor.builder()
				.name("k1.SUM.maxByColor")
				.underlying(k1Sum.getName())
				.aggregationKey(MaxAggregation.KEY)
				.groupBy(GroupByColumns.named("color"))
				.build());
		forest.addMeasure(Partitionor.builder()
				.name("k1.SUM.minByWord")
				.underlying(k1Sum.getName())
				.aggregationKey(MinAggregation.KEY)
				.groupBy(GroupByColumns.named("word"))
				.build());

		ITabularView result = cube()
				.execute(CubeQuery.builder().measure(k1Sum.getName(), "k1.SUM.maxByColor", "k1.SUM.minByWord").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Two rows in the fixture: (azerty, k1=123) and (qwerty, k1=234).
		// k1 grand total = 357, max(by color) of [123, 234] = 234, min(by word) of [123, 234] = 123.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(),
						Map.of(k1Sum.getName(), 0L + 123 + 234, "k1.SUM.maxByColor", 234L, "k1.SUM.minByWord", 123L));
	}
}
