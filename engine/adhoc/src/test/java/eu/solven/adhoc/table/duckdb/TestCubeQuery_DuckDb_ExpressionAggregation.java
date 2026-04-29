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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;

/**
 * Demonstrates querying "delta at the latest/earliest version per {@code (country, color)}" using
 * {@link ExpressionAggregation} — rather than wrapping the raw table in a window-function sub-query in the FROM clause.
 * <p>
 * The physical table holds {@code (country, color, version, delta)} rows with {@code (country, color, version)} as
 * primary key. {@code version} is globally unique across rows so that {@code arg_max}/{@code arg_min} ties are avoided.
 * <p>
 * Two ExpressionAggregation measures are exposed:
 * <ul>
 * <li>{@code delta_latest} → rewritten to DuckDB {@code arg_max("delta", "version")} — "the delta from the row carrying
 * the maximum version within the GROUP BY slice";</li>
 * <li>{@code delta_earliest} → rewritten to DuckDB {@code arg_min("delta", "version")} — symmetric.</li>
 * </ul>
 * A plain {@link SumAggregation} measure {@code delta} is kept alongside as a baseline.
 * <p>
 * <b>Key semantic edge-case — per-group vs. per-pair.</b> Unlike a FROM-clause window that would first filter rows to
 * "one per (country, color)" and then let Adhoc aggregate those survivors, {@code arg_max}/{@code arg_min} are
 * evaluated <em>per GROUP BY slice</em>. The result therefore depends on the grouping:
 * <ul>
 * <li>At grouping {@code (country, color)}: matches the "latest per pair" intent exactly.</li>
 * <li>At a coarser grouping (just {@code country}, just {@code color}, or grand total): returns the arg_max over the
 * <em>whole slice</em> — not the sum of per-pair arg_maxes. A user expecting "collapse-then-sum" semantics for
 * {@code delta_latest} needs a different technique (e.g. a materialised CTE in the FROM clause, as explored in the
 * original design of this test).</li>
 * </ul>
 * <b>Other edge-cases surfaced by the tests:</b>
 * <ul>
 * <li>Adhoc-level filters are applied <em>before</em> the aggregate, so filtering on {@code version} <em>does</em>
 * change which row {@code arg_max} picks — unlike a FROM-clause window where the filter would run outside the
 * window.</li>
 * <li>The aliaser resolves the aggregator's {@code columnName} ({@code delta_latest}) to the raw SQL expression; the
 * {@link ExpressionAggregation} on the Adhoc side only forwards values without re-aggregating them.</li>
 * </ul>
 */
public class TestCubeQuery_DuckDb_ExpressionAggregation extends ADuckDbJooqTest implements IAdhocTestConstants {

	String tableName = "versioned_delta";

	Aggregator deltaSum = Aggregator.builder().name("delta").aggregationKey(SumAggregation.KEY).build();

	Aggregator deltaLatest =
			Aggregator.builder().name("delta_latest").aggregationKey(ExpressionAggregation.KEY).build();

	Aggregator deltaEarliest =
			Aggregator.builder().name("delta_earliest").aggregationKey(ExpressionAggregation.KEY).build();

	/**
	 * Aliaser rewriting the {@code delta_latest} / {@code delta_earliest} aggregator column-names to DuckDB
	 * {@code arg_max}/{@code arg_min} expressions. The expressions reference two physical columns ({@code delta} and
	 * {@code version}) — something a plain {@link SumAggregation} on a single column could not express.
	 */
	protected final ITableAliaser aliaser = MapTableAliaser.builder()
			.aliasToOriginal("delta_latest", "arg_max(\"delta\", \"version\")")
			.aliasToOriginal("delta_earliest", "arg_min(\"delta\", \"version\")")
			.build();

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	@Override
	public CubeWrapper.CubeWrapperBuilder makeCube() {
		return super.makeCube().columnsManager(ColumnsManager.builder().aliaser(aliaser).build());
	}

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(tableName)
				.column("country", SQLDataType.VARCHAR)
				.column("color", SQLDataType.VARCHAR)
				.column("version", SQLDataType.INTEGER)
				.column("delta", SQLDataType.INTEGER)
				.execute();

		// Versions are globally unique across rows so that arg_max / arg_min have a
		// deterministic winner even at the grand-total level.
		// (FR, red) has two versions -> latest(FR,red) = v=2 -> 20, earliest = v=1 -> 10
		insertRow("FR", "red", 1, 10);
		insertRow("FR", "red", 2, 20);

		// (FR, blue) has a single version -> latest = earliest = v=3 -> 30
		insertRow("FR", "blue", 3, 30);

		// (US, red) has a single version -> latest = earliest = v=4 -> 40
		insertRow("US", "red", 4, 40);

		// (US, blue) has two versions -> latest(US,blue) = v=6 -> 60, earliest = v=5 -> 50
		insertRow("US", "blue", 5, 50);
		insertRow("US", "blue", 6, 60);

		forest.addMeasure(deltaSum);
		forest.addMeasure(deltaLatest);
		forest.addMeasure(deltaEarliest);
	}

	/**
	 * Inserts a single fact row into the underlying physical table.
	 */
	protected void insertRow(String country, String color, int version, int delta) {
		dsl.insertInto(DSL
				.table(tableName), DSL.field("country"), DSL.field("color"), DSL.field("version"), DSL.field("delta"))
				.values(country, color, version, delta)
				.execute();
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumns())
				.extracting(c -> c.getName())
				.containsExactlyInAnyOrder("country", "color", "version", "delta")
				.doesNotContain("delta_latest", "delta_earliest");
	}

	/**
	 * Grand total with the three measures. {@code delta} sums all six rows; {@code delta_latest} returns the delta of
	 * the single row with the globally-max version (v=6 → 60); {@code delta_earliest} returns the delta of the
	 * globally- min version (v=1 → 10). This is the clearest illustration of the per-slice semantic of {@code arg_max}.
	 */
	@Test
	public void testGrandTotal_threeMeasures() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(deltaSum, deltaLatest, deltaEarliest).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						Map.of(deltaSum.getName(),
								0L + 10 + 20 + 30 + 40 + 50 + 60,
								deltaLatest.getName(),
								60L,
								deltaEarliest.getName(),
								10L))
				.hasSize(1);
	}

	/**
	 * Grouping by country: {@code arg_max}/{@code arg_min} are scoped to each country-slice. FR's max version is v=3
	 * (FR,blue,30); FR's min version is v=1 (FR,red,10). US's max version is v=6 (US,blue,60); US's min version is v=4
	 * (US,red,40).
	 * <p>
	 * Note: FR's "latest" is {@code (FR,blue)} even though {@code (FR,red)} also has a v=2 row — the per-country
	 * arg_max picks the single highest version, not one per (country,color) pair.
	 */
	@Test
	public void testGroupByCountry_threeMeasures() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure(deltaSum, deltaLatest, deltaEarliest).groupByAlso("country").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR"),
						Map.of(deltaSum.getName(),
								0L + 10 + 20 + 30,
								deltaLatest.getName(),
								30L,
								deltaEarliest.getName(),
								10L))
				.containsEntry(Map.of("country", "US"),
						Map.of(deltaSum.getName(),
								0L + 40 + 50 + 60,
								deltaLatest.getName(),
								60L,
								deltaEarliest.getName(),
								40L))
				.hasSize(2);
	}

	/**
	 * Grouping by (country, color): this is the only grouping at which {@code delta_latest}/{@code delta_earliest}
	 * match the "per-pair latest/earliest" intent — because the GROUP BY slice coincides with the partition key.
	 */
	@Test
	public void testGroupByCountryAndColor_matchesPerPairIntent() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(deltaSum, deltaLatest, deltaEarliest)
				.groupByAlso("country")
				.groupByAlso("color")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR", "color", "red"),
						Map.of(deltaSum
								.getName(), 0L + 10 + 20, deltaLatest.getName(), 20L, deltaEarliest.getName(), 10L))
				.containsEntry(Map.of("country", "FR", "color", "blue"),
						Map.of(deltaSum.getName(), 0L + 30, deltaLatest.getName(), 30L, deltaEarliest.getName(), 30L))
				.containsEntry(Map.of("country", "US", "color", "red"),
						Map.of(deltaSum.getName(), 0L + 40, deltaLatest.getName(), 40L, deltaEarliest.getName(), 40L))
				.containsEntry(Map.of("country", "US", "color", "blue"),
						Map.of(deltaSum
								.getName(), 0L + 50 + 60, deltaLatest.getName(), 60L, deltaEarliest.getName(), 50L))
				.hasSize(4);
	}

	/**
	 * Edge-case: a filter on {@code version} is applied <em>before</em> the aggregate, so {@code arg_max} picks the max
	 * <em>within the filtered subset</em>. Filtering to {@code version<=2} leaves only (FR,red,v1,10) and
	 * (FR,red,v2,20), so even the grand-total {@code delta_latest} is now 20 (from v=2) instead of 60.
	 * <p>
	 * This is the opposite of the FROM-clause-window variant, where filters would run <em>outside</em> the window and
	 * never change which row was picked as "latest".
	 */
	@Test
	public void testFilterOnVersion_shiftsArgMaxWinner() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(deltaSum, deltaLatest, deltaEarliest)
				.filter(ColumnFilter.builder().column("version").matchIn(java.util.Set.of(1, 2)).build())
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						Map.of(deltaSum
								.getName(), 0L + 10 + 20, deltaLatest.getName(), 20L, deltaEarliest.getName(), 10L))
				.hasSize(1);
	}

	/**
	 * Edge-case: filtering on a single country makes {@code arg_max} scope its "max version" to that country only. FR
	 * has rows v=1, v=2, v=3 → latest is v=3 → 30, earliest is v=1 → 10.
	 */
	@Test
	public void testFilterOnCountry_scopesArgMaxToCountry() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(deltaSum, deltaLatest, deltaEarliest)
				.filter(ColumnFilter.matchEq("country", "FR"))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(),
						Map.of(deltaSum.getName(),
								0L + 10 + 20 + 30,
								deltaLatest.getName(),
								30L,
								deltaEarliest.getName(),
								10L))
				.hasSize(1);
	}

}
