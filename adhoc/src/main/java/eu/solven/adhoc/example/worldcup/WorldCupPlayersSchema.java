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
package eu.solven.adhoc.example.worldcup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jooq.AggregateFunction;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.core.io.ClassPathResource;

import com.google.common.base.CharMatcher;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.engine.context.GeneratedColumnsPreparator;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.combination.ConstantCombination;
import eu.solven.adhoc.measure.decomposition.CumulatingDecomposition;
import eu.solven.adhoc.measure.decomposition.DuplicatingDecomposition;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqSnowflakeSchemaBuilder;
import eu.solven.adhoc.table.sql.JooqTableCapabilities;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the schema for WorldCupPlayers example.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings({ "PMD.AvoidDuplicateLiterals", "checkstyle:MagicNumber" })
public class WorldCupPlayersSchema {
	public String getName() {
		return "WorldCupPlayers";
	}

	public IMeasureForest getForest(String forestName) {

		List<IMeasure> measures = new ArrayList<>();
		measures.add(Aggregator.countAsterisk());
		measures.add(Aggregator.builder()
				.name("events")
				.aggregationKey(EventAggregation.class.getName())
				.columnName("Event")
				.build());

		measures.add(Dispatchor.builder()
				.name("event_count")
				.decompositionKey(DispatchedEvents.class.getName())
				.underlying("events")
				.build());

		measures.add(Filtrator.builder()
				.name("goal_count")
				// Goals and OwnGoals
				.filter(ColumnFilter.matchIn("event_code", "G", "OG"))
				.underlying("event_count")
				.build());

		measures.add(Filtrator.builder()
				.name("redcard_count")
				// Redcards and SecondYellows
				.filter(ColumnFilter.matchIn("event_code", "R", "SY"))
				.underlying("event_count")
				.build());

		measures.add(Partitionor.builder()
				.name("match_count")
				.groupBy(GroupByColumns.named("MatchId"))
				.combinationKey(ConstantCombination.class.getName())
				.combinationOption(ConstantCombination.K_CONSTANT, 1)
				.underlying("events")
				.build());
		measures.add(Partitionor.builder()
				.name("player_score")
				.combinationKey(EventsScoreCombination.class.getName())
				.groupBy(GroupByColumns.named("Player name"))
				.underlyings(List.of("goal_count", "redcard_count", "match_count"))
				.aggregationKey(AvgAggregation.KEY)
				.build());
		measures.add(Partitionor.builder()
				.name("coach_score")
				.combinationKey(EventsScoreCombination.class.getName())
				.groupBy(GroupByColumns.named("Coach name"))
				.underlyings(List.of("goal_count", "redcard_count", "match_count"))
				.aggregationKey(AvgAggregation.KEY)
				.build());

		Stream.of("match_count", "player_score", "coach_score").forEach(measure -> {
			measures.add(Shiftor.builder()
					.name(measure + ".previousWorldCup")
					.editorKey(SimpleFilterEditor.KEY)
					.editorOptions(Map.of(SimpleFilterEditor.P_SHIFTED, Map.of("year", shitYearFunction())))
					.underlying(measure)
					.build());
		});

		Stream.of("match_count", "player_score", "coach_score").forEach(measure -> {
			measures.add(Shiftor.builder()
					.name(measure + ".sinceInception")
					.editorKey(SimpleFilterEditor.KEY)
					.editorOptions(Map.of(SimpleFilterEditor.P_SHIFTED, Map.of("year", upToFunction())))
					.underlying(measure)
					.build());
		});

		// TODO Such an hardcoded list should not be necessary
		List<Object> years = List.of(1990L, 1994L, 1998L, 2002L);

		Stream.of("match_count", "player_score", "coach_score").forEach(measure -> {
			measures.add(Dispatchor.builder()
					.name(measure + ".sinceInception2")
					.decompositionKey(CumulatingDecomposition.class.getName())
					.decompositionOption(DuplicatingDecomposition.K_COLUMN_TO_COORDINATES, Map.of("year", years))
					.decompositionOption(CumulatingDecomposition.K_FILTER_EDITOR, upToEditor())
					.underlying(measure)
					.build());
		});

		return MeasureForest.fromMeasures(forestName, measures);
	}

	@SuppressWarnings({ "checkstyle:AvoidInlineConditionals", "checkstyle:MagicNumber" })
	protected Function<Object, Object> shitYearFunction() {
		return t -> t instanceof Number n ? n.longValue() - 4L : t;
	}

	@SuppressWarnings({ "checkstyle:AvoidInlineConditionals" })
	protected Function<Object, Object> upToFunction() {
		return t -> t instanceof Comparable<?> c
				? ComparingMatcher.builder().greaterThan(false).matchIfEqual(true).operand(c).build()
				: t;
	}

	@SuppressWarnings({ "checkstyle:AvoidInlineConditionals" })
	protected IFilterEditor upToEditor() {
		return f -> SimpleFilterEditor.shiftIfPresent(f, "year", upToFunction());
	}

	public ITableWrapper getTable(String tableName) {
		DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();

		DSLContext dslContext = dslSupplier.getDSLContext();

		dslContext.connection(connection -> {
			loadParquetToTable(connection, new ClassPathResource("/datasets/worldcup/WorldCupPlayers.parquet"));
			loadParquetToTable(connection, new ClassPathResource("/datasets/worldcup/WorldCupMatches.parquet"));
		});

		JooqSnowflakeSchemaBuilder worldCupPlayers = snowflakeBuilder();

		JooqTableWrapperParameters tableParameters = JooqTableWrapperParameters.builder()
				.table(worldCupPlayers.getSnowflakeTable())
				.dslSupplier(dslSupplier)
				.build();

		return new JooqTableWrapper(tableName, tableParameters) {
			@Override
			protected IJooqTableQueryFactory makeQueryFactory(DSLContext dslContext) {
				return new JooqTableQueryFactory(tableParameters.getOperatorFactory(),
						tableParameters.getTable(),
						dslContext,
						JooqTableCapabilities.from(dslContext.dialect())) {

					@Override
					protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator,
							Field<Object> fieldToAggregate) {
						if (aggregator.getAggregationKey().equals(EventAggregation.class.getName())) {
							// https://duckdb.org/docs/stable/sql/functions/aggregates.html#arg_maxarg-val-n
							return DSL.aggregate("array_agg", Object.class, fieldToAggregate);
						} else {
							return super.onCustomAggregation(aggregator, fieldToAggregate);
						}
					}
				};
			}
		};
	}

	private JooqSnowflakeSchemaBuilder snowflakeBuilder() {
		return JooqSnowflakeSchemaBuilder.builder()
				.baseTable(DSL.table("WorldCupPlayers"))
				.baseTableAlias("WorldCupPlayers")
				.build()
				.leftJoin(DSL.table("WorldCupMatches"),
						"WorldCupMatches",
						List.of(Map.entry("MatchId", "MatchID"), Map.entry("RoundId", "RoundID")));
	}

	// `SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE` as we can not use PreparedStatement on FROM clause.
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void loadParquetToTable(Connection connection, ClassPathResource resource)
			throws IOException, SQLException {
		String fileName = resource.getFilename();
		String simpleName = fileName.substring(0, fileName.lastIndexOf('.'));

		Path tmpPath = Files.createTempDirectory("adhoc-" + this.getClass().getSimpleName());

		Path parquetAsPath = tmpPath.resolve(fileName);
		Files.copy(resource.getInputStream(), parquetAsPath);

		try (Statement s = connection.createStatement()) {
			String sql = makeSanitizedSql(simpleName, parquetAsPath);
			s.execute(sql);
		}

		// Delete the file as it is loaded in-memory in DuckDB
		boolean deleted = parquetAsPath.toFile().delete();
		log.debug("deleted={} for path={}", deleted, tmpPath);
	}

	protected String makeSanitizedSql(String simpleName, Path parquetAsPath) {
		if (CharMatcher.anyOf("' ").matchesAnyOf(simpleName)) {
			throw new IllegalArgumentException("Invalid tableName: %s".formatted(simpleName));
		}

		String parquetPathAsString = parquetAsPath.toAbsolutePath().toString();
		if (CharMatcher.anyOf("'").matchesAnyOf(simpleName)) {
			throw new IllegalArgumentException("Invalid tableName: %s".formatted(simpleName));
		}

		return "CREATE TABLE %s AS (SELECT * FROM '%s');".formatted(simpleName, parquetPathAsString);
	}

	public CubeWrapperBuilder makeCube(AdhocSchema schema,
			WorldCupPlayersSchema worldCupSchema,
			ITableWrapper table,
			IMeasureForest forest) {
		return schema.openCubeWrapperBuilder()
				.name(worldCupSchema.getName())
				.forest(forest)
				.table(table)
				.queryPreparator(GeneratedColumnsPreparator.builder().generatedColumnsMeasure("event_count").build())
				.columnsManager(ColumnsManager.builder()
						.aliaser(MapTableAliaser.builder()
								.aliasToOriginals(snowflakeBuilder().getAliasToOriginal())
								// TODO Need to progress on caseSensitivity
								.aliasToOriginal("MatchID", "WorldCupPlayers.MatchId")
								.aliasToOriginal("RoundID", "WorldCupPlayers.RoundId")
								.build())
						.build());
	}
}
