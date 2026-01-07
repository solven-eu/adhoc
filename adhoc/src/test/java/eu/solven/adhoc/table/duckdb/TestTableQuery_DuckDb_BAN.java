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

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.util.AdhocBenchmark;
import lombok.extern.slf4j.Slf4j;

/**
 * Some integrations tests over `https://www.data.gouv.fr/fr/datasets/ban-format-parquet/`. It is a ~500MB Parquet
 * files, with a 26 million cardinality `id` field and a bunch of fields meaningful to any human.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TestTableQuery_DuckDb_BAN extends ADuckDbJooqTest implements IAdhocTestConstants {

	public static String PATH_TO_BAN = "/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet";

	@BeforeAll
	public static void assumeFileisPresent() {
		boolean banIsPresent = Paths.get(PATH_TO_BAN).toFile().isFile();
		Assumptions.assumeTrue(banIsPresent, "BAN file is not present");
		if (!banIsPresent) {
			log.info("BAN file ({}) is not present: some benchmarks are skipped", PATH_TO_BAN);
		}
	}

	String tableName = "addressesFrance";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				JooqTableWrapperParameters.builder()
						.dslSupplier(dslSupplier)
						.table(DSL.table(
								"read_parquet('/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet')"))
						.build());
	}

	@BeforeEach
	public void registerMeasures() {
		forest.addMeasure(Aggregator.countAsterisk());
	}

	// Sanity Check to ensure the dataset is large
	@Test
	public void testGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(Aggregator.countAsterisk().getName()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 26_045_333L));
	}

	@Test
	public void testGetColumns() {
		Assertions.assertThat(cube().getColumnTypes()).hasSize(21);
	}

	@Test
	public void testCoordinates_giganticCardinality() {
		CoordinatesSample columnMeta = cube().getCoordinates("id", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality())
				.isCloseTo(26_045_333, Percentage.withPercentage(10));
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(100);
	}

	@Test
	public void testCoordinates_blob() {
		CoordinatesSample columnMeta = cube().getCoordinates("geom", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality())
				.isCloseTo(26_045_333, Percentage.withPercentage(20));

		// We skip returning Blob
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(0);
	}

	@Test
	public void testCoordinates_ubigint() {
		CoordinatesSample columnMeta = cube().getCoordinates("h3_7", IValueMatcher.MATCH_ALL, 100);

		Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(107_635, Percentage.withPercentage(10));
		Assertions.assertThat(columnMeta.getCoordinates()).hasSize(100);
	}

	@AdhocBenchmark
	@Test
	public void testCoordinates_allColumns_like() {
		// Search through all columns for any coordinate matching `%abc%`
		Map<String, IValueMatcher> columnsToAbc = new HashMap<>();
		cube().getColumnTypes().forEach((column, type) -> columnsToAbc.put(column, LikeMatcher.matching("%ab%")));

		Map<String, CoordinatesSample> columnsMeta = cube().getCoordinates(columnsToAbc, 5);

		Assertions.assertThat(columnsMeta).hasSize(21).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("rep");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(12, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_voie");
			Assertions.assertThat(columnMeta.getEstimatedCardinality())
					.isCloseTo(16_511, Percentage.withPercentage(20));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_commune");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(329, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		}).anySatisfy((c, columnMeta) -> {
			Assertions.assertThat(c).isEqualTo("nom_ld");
			Assertions.assertThat(columnMeta.getEstimatedCardinality()).isCloseTo(1897, Percentage.withPercentage(10));
			Assertions.assertThat(columnMeta.getCoordinates()).hasSize(5);
		});
	}

}
