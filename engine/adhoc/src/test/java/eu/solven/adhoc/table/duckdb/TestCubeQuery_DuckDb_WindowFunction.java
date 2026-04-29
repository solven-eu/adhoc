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
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.pepper.collection.MapWithNulls;

/**
 * Business case requiring a SQL window function: for each country, expose only the row with the latest version, then
 * enrich that row with color-metadata coming from a second table via a LEFT JOIN.
 * <p>
 * The physical layout is two tables:
 * <ul>
 * <li>{@code versioned_country(country, version, color, delta)} — fact table with {@code (country, version)} as primary
 * key. {@code color} and {@code delta} vary with the version.</li>
 * <li>{@code color_metadata(color, meta)} — enrichment table keyed on {@code color}.</li>
 * </ul>
 * The Adhoc-facing {@code TableLike} is a derived table that
 * <ol>
 * <li>ranks rows per country on {@code version DESC} via {@code ROW_NUMBER() OVER (PARTITION BY country ORDER BY
 * version DESC)};</li>
 * <li>keeps only {@code rn=1} — i.e. the latest row per country;</li>
 * <li>LEFT JOINs the enrichment table on {@code color} to expose the {@code meta} column.</li>
 * </ol>
 * Adhoc then queries this derived table as if it were a normal, pre-computed "latest per country + enrichment" view, so
 * slices like {@code GROUP BY country}, {@code GROUP BY color}, {@code GROUP BY meta} all naturally project the
 * latest-version facts.
 * <p>
 * This is the actual use-case that motivates pushing window logic into the FROM clause: unlike the
 * {@code arg_max}-based {@code ExpressionAggregation} variant, here the window-filtered row-set is a stable "view" that
 * composes cleanly with joins and any grouping level — a user grouping by just {@code country} still sees the
 * latest-version delta for that country, and the LEFT JOIN naturally preserves countries whose latest-row color has no
 * enrichment.
 */
public class TestCubeQuery_DuckDb_WindowFunction extends ADuckDbJooqTest implements IAdhocTestConstants {

	String factTable = "versioned_country";
	String enrichmentTable = "color_metadata";

	Aggregator deltaSum = Aggregator.builder().name("delta").aggregationKey(SumAggregation.KEY).build();

	/**
	 * Builds the derived {@code TableLike} that Adhoc queries against: latest row per country, LEFT-JOINed on
	 * {@code color} against the enrichment table.
	 *
	 * @return a jOOQ table wrapping the window + join
	 */
	protected Table<?> latestPerCountryWithEnrichment() {
		Table<?> ranked = DSL.select(DSL.field("country"),
				DSL.field("version"),
				DSL.field("color"),
				DSL.field("delta"),
				DSL.rowNumber()
						.over(DSL.partitionBy(DSL.field("country")).orderBy(DSL.field("version").desc()))
						.as("rn"))
				.from(DSL.table(DSL.name(factTable)))
				.asTable("ranked");

		Table<?> latest = DSL
				.select(ranked.field("country"), ranked.field("version"), ranked.field("color"), ranked.field("delta"))
				.from(ranked)
				.where(DSL.field("rn", Integer.class).eq(1))
				.asTable("latest");

		return latest.leftJoin(DSL.table(DSL.name(enrichmentTable)).as("m")).using(DSL.field("color"));
	}

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(factTable,
				DuckDBHelper.parametersBuilder(dslSupplier).table(latestPerCountryWithEnrichment()).build());
	}

	@BeforeEach
	public void initDataAndMeasures() {
		dsl.createTableIfNotExists(factTable)
				.column("country", SQLDataType.VARCHAR)
				.column("version", SQLDataType.INTEGER)
				.column("color", SQLDataType.VARCHAR)
				.column("delta", SQLDataType.INTEGER)
				.execute();
		dsl.createTableIfNotExists(enrichmentTable)
				.column("color", SQLDataType.VARCHAR)
				.column("meta", SQLDataType.VARCHAR)
				.execute();

		// FR evolves red -> blue -> green. The window keeps only v=3 (green, 30).
		insertFact("FR", 1, "red", 10);
		insertFact("FR", 2, "blue", 20);
		insertFact("FR", 3, "green", 30);

		// US has a single version. Latest = v=1 (red, 40).
		insertFact("US", 1, "red", 40);

		// DE evolves blue -> blue. Latest = v=2 (blue, 60). Same color across versions on purpose.
		insertFact("DE", 1, "blue", 50);
		insertFact("DE", 2, "blue", 60);

		// IT's latest color is purple — which has no enrichment row, so the LEFT JOIN yields meta=NULL.
		insertFact("IT", 1, "purple", 70);

		// ES shares the "red" latest-color with US, which lets colour/meta slices sum multiple countries.
		insertFact("ES", 1, "red", 80);

		// Enrichment: purple is deliberately absent so IT surfaces the LEFT-JOIN-null edge case.
		insertMeta("red", "warm");
		insertMeta("blue", "cold");
		insertMeta("green", "neutral");

		forest.addMeasure(deltaSum);
	}

	/**
	 * Inserts a single fact row into the versioned storage.
	 */
	protected void insertFact(String country, int version, String color, int delta) {
		dsl.insertInto(DSL
				.table(factTable), DSL.field("country"), DSL.field("version"), DSL.field("color"), DSL.field("delta"))
				.values(country, version, color, delta)
				.execute();
	}

	/**
	 * Inserts a single enrichment row into the color-metadata table.
	 */
	protected void insertMeta(String color, String meta) {
		dsl.insertInto(DSL.table(enrichmentTable), DSL.field("color"), DSL.field("meta")).values(color, meta).execute();
	}

	/**
	 * Adhoc sees the 5 columns produced by the derived table: {@code country, version, color, delta, meta}. The
	 * {@code rn} rank column is dropped by the outer projection, and the join uses {@code USING(color)} so
	 * {@code color} appears only once.
	 */
	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumns())
				.extracting(c -> c.getName())
				.containsExactlyInAnyOrder("country", "version", "color", "delta", "meta");
	}

	/**
	 * Grand total: sum of latest-version deltas across all 5 countries = 30 + 40 + 60 + 70 + 80 = 280.
	 */
	@Test
	public void testGrandTotal() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(deltaSum).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(deltaSum.getName(), 0L + 30 + 40 + 60 + 70 + 80))
				.hasSize(1);
	}

	/**
	 * Group by country: each country is represented by its latest-version row exactly once, regardless of how many
	 * historical versions exist.
	 */
	@Test
	public void testGroupByCountry() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(deltaSum).groupByAlso("country").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR"), Map.of(deltaSum.getName(), 0L + 30))
				.containsEntry(Map.of("country", "US"), Map.of(deltaSum.getName(), 0L + 40))
				.containsEntry(Map.of("country", "DE"), Map.of(deltaSum.getName(), 0L + 60))
				.containsEntry(Map.of("country", "IT"), Map.of(deltaSum.getName(), 0L + 70))
				.containsEntry(Map.of("country", "ES"), Map.of(deltaSum.getName(), 0L + 80))
				.hasSize(5);
	}

	/**
	 * Group by (country, version): shows that each country is paired with exactly one version — the latest one. FR's
	 * historical v=1 and v=2 rows are invisible because the window dropped them.
	 */
	@Test
	public void testGroupByCountryAndVersion_keepsOnlyLatestVersion() {
		ITabularView result = cube()
				.execute(CubeQuery.builder().measure(deltaSum).groupByAlso("country").groupByAlso("version").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// Note: `version` comes back as Long even though the underlying column is INTEGER, because DuckDB's ROW_NUMBER
		// wrapper widens the projection's numeric type.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR", "version", 3L), Map.of(deltaSum.getName(), 0L + 30))
				.containsEntry(Map.of("country", "US", "version", 1L), Map.of(deltaSum.getName(), 0L + 40))
				.containsEntry(Map.of("country", "DE", "version", 2L), Map.of(deltaSum.getName(), 0L + 60))
				.containsEntry(Map.of("country", "IT", "version", 1L), Map.of(deltaSum.getName(), 0L + 70))
				.containsEntry(Map.of("country", "ES", "version", 1L), Map.of(deltaSum.getName(), 0L + 80))
				.hasSize(5);
	}

	/**
	 * Group by color: the color is the latest-version color per country. US (red) + ES (red) sum to 120; FR (green), DE
	 * (blue), IT (purple) contribute one country each.
	 */
	@Test
	public void testGroupByLatestColor() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(deltaSum).groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("color", "green"), Map.of(deltaSum.getName(), 0L + 30))
				.containsEntry(Map.of("color", "red"), Map.of(deltaSum.getName(), 0L + 40 + 80))
				.containsEntry(Map.of("color", "blue"), Map.of(deltaSum.getName(), 0L + 60))
				.containsEntry(Map.of("color", "purple"), Map.of(deltaSum.getName(), 0L + 70))
				.hasSize(4);
	}

	/**
	 * Group by enrichment column {@code meta}: countries whose latest color has a matching enrichment row are grouped
	 * by their enrichment label. IT (purple, no enrichment) surfaces as a {@code meta=null} slice — Adhoc does
	 * <em>not</em> drop null coordinates here, so callers still see the "unmapped" contribution from LEFT-JOIN misses.
	 */
	@Test
	public void testGroupByMeta_nullEnrichmentIsPreserved() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(deltaSum).groupByAlso("meta").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("meta", "warm"), Map.of(deltaSum.getName(), 0L + 40 + 80))
				.containsEntry(Map.of("meta", "cold"), Map.of(deltaSum.getName(), 0L + 60))
				.containsEntry(Map.of("meta", "neutral"), Map.of(deltaSum.getName(), 0L + 30))
				.containsEntry(MapWithNulls.of("meta", null), Map.of(deltaSum.getName(), 0L + 70))
				.hasSize(4);
	}

	/**
	 * Edge-case: filtering on {@code version=2} returns only DE. FR also has a historical v=2 row in the physical
	 * table, but the window already dropped it (FR's latest is v=3), so it never reaches the Adhoc layer. This is the
	 * key difference with a plain table: the window turns "version" into a coordinate that identifies the latest-row
	 * version, not any arbitrary historical version.
	 */
	@Test
	public void testFilterOnVersion_onlyLatestRowsVisible() {
		ITabularView result = cube()
				.execute(CubeQuery.builder().measure(deltaSum).filter(ColumnFilter.matchEq("version", 2)).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(deltaSum.getName(), 0L + 60))
				.hasSize(1);
	}

	/**
	 * Edge-case: filtering on the enrichment column {@code meta=warm} returns US + ES (both red, whose latest color
	 * maps to "warm"). This proves the LEFT JOIN side is fully first-class for Adhoc: it is filterable and groupable
	 * just like a native column.
	 */
	@Test
	public void testFilterOnMeta() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure(deltaSum)
				.filter(ColumnFilter.matchEq("meta", "warm"))
				.groupByAlso("country")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("country", "US"), Map.of(deltaSum.getName(), 0L + 40))
				.containsEntry(Map.of("country", "ES"), Map.of(deltaSum.getName(), 0L + 80))
				.hasSize(2);
	}

}
