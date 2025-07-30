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
package eu.solven.adhoc.pivotable.app.example;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBSingleValueAppender;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.table.composite.CompositeCubesTableWrapper;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqSnowflakeSchemaBuilder;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;
import eu.solven.pepper.spring.PepperResourceHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Add a simple cube for tests and demo purposes. Requires a `self` schema to be available in Spring
 * {@link ApplicationContext}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class InjectPixarExampleCubesConfig {

	// `java:S6831` as Sonar states `@Qualifier` is bad on `@Bean`
	@Profile(IPivotableSpringProfiles.P_SIMPLE_DATASETS)
	@Bean
	public Void initPixarCubes(@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT) AdhocSchema schema) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerPixarFilms(schema);

		return null;
	}

	@SuppressWarnings("checkstyle.MethodLength")
	protected void registerPixarFilms(AdhocSchema schema) {
		DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();

		DSLContext dslContext = dslSupplier.getDSLContext();

		// number,film,release_date,run_time,film_rating,plot
		// 1,Toy Story,1995-11-22,81,G,A cowboy doll is profoundly threatened and jealous when a new spaceman action
		// figure supplants him as top toy in a boy's bedroom.
		dslContext.createTable("pixar_films")
				.column("number", SQLDataType.INTEGER)
				.column("film", SQLDataType.VARCHAR)
				.column("release_date", SQLDataType.LOCALDATE)
				.column("run_time", SQLDataType.INTEGER)
				.column("film_rating", SQLDataType.VARCHAR)
				.column("plot", SQLDataType.VARCHAR)
				.execute();

		// film,role_type,name
		// Toy Story,Director,John Lasseter
		dslContext.createTable("pixar_people")
				.column("film", SQLDataType.VARCHAR)
				.column("role_type", SQLDataType.VARCHAR)
				.column("name", SQLDataType.VARCHAR)
				.execute();

		dslContext.connection(connection -> {
			DuckDBConnection duckDbC = (DuckDBConnection) connection;

			Stream.of("pixar_films", "pixar_people").forEach(fileName -> {
				// https://github.com/duckdb/duckdb-java/issues/310
				try (DuckDBSingleValueAppender appender =
						duckDbC.createSingleValueAppender(DuckDBConnection.DEFAULT_SCHEMA, fileName)) {
					String csv = PepperResourceHelper.loadAsString("/datasets/Pixar+Films/%s.csv".formatted(fileName),
							StandardCharsets.UTF_8);

					CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

					try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(csv)).withSkipLines(1)
							.withCSVParser(parser)
							.build()) {
						csvReader.forEach(row -> {
							try {
								appender.beginRow();
								Stream.of(row).forEach(cell -> {
									try {
										appender.append(cell);
									} catch (SQLException e) {
										throw new IllegalStateException(
												"Issue loading cell=%s for file=%s".formatted(cell, fileName),
												e);
									}
								});
								appender.endRow();
							} catch (SQLException e) {
								throw new IllegalStateException("Issue loading " + fileName, e);
							}
						});
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}

				} catch (SQLException e) {
					throw new IllegalStateException("Issue processing tableName=%s".formatted(fileName), e);
				}
			});

		});

		CubeWrapper filmsCube;
		{

			JooqSnowflakeSchemaBuilder films = JooqSnowflakeSchemaBuilder.builder()
					.baseTable(DSL.table("pixar_films"))
					.baseTableAlias("films")
					.build();

			JooqTableWrapper filmsTable = new JooqTableWrapper("films",
					JooqTableWrapperParameters.builder()
							.table(films.getSnowflakeTable())
							.dslSupplier(dslSupplier)
							.build());
			schema.registerTable(filmsTable);

			List<IMeasure> measuresFilm = new ArrayList<>();
			measuresFilm.add(Aggregator.countAsterisk().toBuilder().name("count(films)").build());

			schema.registerForest(MeasureForest.fromMeasures("films", measuresFilm));
			filmsCube = schema.openCubeWrapperBuilder()
					.name("films")
					.forest(MeasureForest.fromMeasures("films", measuresFilm))
					.table(filmsTable)
					.columnsManager(ColumnsManager.builder()
							.transcoder(MapTableTranscoder.builder()
									.queriedToUnderlyings(films.getQueriedToUnderlying())
									.build())
							.build())
					.build();
			schema.registerCube(filmsCube);
		}

		CubeWrapper peopleCube;
		{
			JooqSnowflakeSchemaBuilder people = JooqSnowflakeSchemaBuilder.builder()
					.baseTable(DSL.table("pixar_people"))
					.baseTableAlias("people")
					.build()
					.leftJoin(DSL.table("pixar_films"), "films", List.of(Map.entry("film", "film")));

			JooqTableWrapper peopleTable = new JooqTableWrapper("people",
					JooqTableWrapperParameters.builder()
							.table(people.getSnowflakeTable())
							.dslSupplier(dslSupplier)
							.build());
			schema.registerTable(peopleTable);

			List<IMeasure> measuresPeople = new ArrayList<>();
			measuresPeople.add(Aggregator.countAsterisk().toBuilder().name("count(people)").build());

			schema.registerForest(MeasureForest.fromMeasures("people", measuresPeople));
			peopleCube = schema.openCubeWrapperBuilder()
					.name("people")
					.forest(MeasureForest.fromMeasures("people", measuresPeople))
					.table(peopleTable)
					.columnsManager(ColumnsManager.builder()
							.transcoder(MapTableTranscoder.builder()
									.queriedToUnderlyings(people.getQueriedToUnderlying())
									.build())
							.build())
					.build();
			schema.registerCube(peopleCube);
		}

		{
			CompositeCubesTableWrapper pixarTable =
					CompositeCubesTableWrapper.builder().name("pixar").cube(filmsCube).cube(peopleCube).build();
			schema.registerTable(pixarTable);

			List<IMeasure> measuresComposite = new ArrayList<>();
			measuresComposite.add(Combinator.builder()
					.name("people per film")
					.combinationKey(DivideCombination.KEY)
					.underlying("count(people)")
					.underlying("count(films)")
					.build());

			// `raw` includes only the additional measures of the composite cube, but no reference to the underlying
			// cubes
			// measures available through the composite
			MeasureForest rawPixarForest = MeasureForest.fromMeasures("pixar", measuresComposite);
			// `clear` also includes the underlying measures, available through the composite
			IMeasureForest clearPixarForest = pixarTable.injectUnderlyingMeasures(rawPixarForest);

			schema.registerForest(clearPixarForest);
			schema.registerCube("pixar", "pixar", "pixar");
		}
	}
}
