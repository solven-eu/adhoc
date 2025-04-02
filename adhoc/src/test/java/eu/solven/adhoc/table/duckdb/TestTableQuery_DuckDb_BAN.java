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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.AdhocTestHelper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Some integrations tests over `https://www.data.gouv.fr/fr/datasets/ban-format-parquet/`. It is a ~500MB Parquet
 * files, with a 26 million cardinality `id` field and a bunch of fields meaningful to any human.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TestTableQuery_DuckDb_BAN extends ADagTest implements IAdhocTestConstants {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	public static String PATH_TO_BAN = "/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet";

	@BeforeAll
	public static void assumeFileisPresent() {
		boolean banIsPresent = Paths.get(PATH_TO_BAN).toFile().isFile();
		Assumptions.assumeTrue(banIsPresent, "BAN file is not present");
		log.info("BAN file ({}) is not present: some benchmarks are skipped", PATH_TO_BAN);
	}

	String tableName = "addressesFrance";

	JooqTableWrapper table = new JooqTableWrapper(tableName,
			JooqTableWrapperParameters.builder()
					.dslSupplier(DuckDbHelper.inMemoryDSLSupplier())
					.table(DSL.table(
							"read_parquet('/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet')"))
					.build());

	private CubeWrapper wrapInCube(IMeasureForest forest) {
		AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(AdhocTestHelper.eventBus()::post).build();

		return CubeWrapper.builder().engine(aqe).forest(forest).table(table).engine(aqe).build();
	}

	@Override
	public void feedTable() {
		// nothing to feed
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Aggregator.countAsterisk());
	}

	// Sanity Check to ensure the dataset is large
	@Test
	public void testGrandTotal() {
		ITabularView output =
				wrapInCube(forest).execute(AdhocQuery.builder().measure(Aggregator.countAsterisk().getName()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 26_045_333L));
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(wrapInCube(forest).getColumns()).hasSize(21);
	}

	@Test
	public void testCoordinates_giganticCardinality() {
		CoordinatesSample columnMeta = wrapInCube(forest).getCoordinates("id", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality())
				.isCloseTo(26_045_333, Percentage.withPercentage(10));
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(100);
	}

	@Test
	public void testCoordinates_blob() {
		CoordinatesSample columnMeta = wrapInCube(forest).getCoordinates("geom", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality())
				.isCloseTo(26_045_333, Percentage.withPercentage(10));

		// We skip returning Blob
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(0);
	}

	@Test
	public void testCoordinates_ubigint() {
		CoordinatesSample columnMeta = wrapInCube(forest).getCoordinates("h3_7", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(107_635, Percentage.withPercentage(10));
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(100);
	}

	@Test
	public void testCoordinates_allColumns_like() {
		// Search through all columns for any coordinate matching `%abc%`
		Map<String, IValueMatcher> columnsToAbc = new HashMap<>();
		wrapInCube(forest).getColumns()
				.forEach((column, type) -> columnsToAbc.put(column, LikeMatcher.matching("%ab%")));

		Map<String, CoordinatesSample> columnsMeta = wrapInCube(forest).getCoordinates(columnsToAbc, 5);

		Assertions.assertThat(columnsMeta).hasSize(21).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("rep");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(12, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_voie");
			Assertions.assertThat(columnMeta.getEstimatedCardinality())
					.isCloseTo(20_763, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_commune");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(292, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_ld");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(2324, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		});
	}

}
