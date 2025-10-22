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
package eu.solven.adhoc.table.duckdb.worldcup;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import com.google.common.collect.Sets;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.RelevancyHeuristic;
import eu.solven.adhoc.beta.schema.RelevancyHeuristic.CubeRelevancy;
import eu.solven.adhoc.beta.schema.RelevancyHeuristic.MeasureRelevancy;
import eu.solven.adhoc.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.coordinate.CalculatedCoordinate;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.context.GeneratedColumnsPreparator;
import eu.solven.adhoc.example.worldcup.WorldCupPlayersSchema;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.transformator.MapWithNulls;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.duckdb.ADuckDbJooqTest;
import eu.solven.adhoc.util.IStopwatchFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * 
 * @author Benoit Lacelle
 * @see InjectWorldCupExampleCubesConfig
 */
@Slf4j
public class TestTableQuery_DuckDb_WorldCup extends ADuckDbJooqTest implements IAdhocTestConstants {

	WorldCupPlayersSchema worldCupSchema = new WorldCupPlayersSchema();

	IMeasureForest forest = worldCupSchema.getForest(worldCupSchema.getName());

	@Override
	public ITableWrapper makeTable() {
		return worldCupSchema.getTable(worldCupSchema.getName());
	}

	@Override
	public CubeWrapperBuilder makeCube() {
		return worldCupSchema.makeCube(AdhocSchema.builder().engine(engine()).build(), worldCupSchema, table(), forest)
				.queryPreparator(GeneratedColumnsPreparator.builder().generatedColumnsMeasure("event_count").build());
	}

	// count rows
	@Test
	public void testGrandTotal() {
		ITabularView result = cube().execute(CubeQuery.builder().measure(countAsterisk.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry(countAsterisk.getName(), 39_256L).hasSize(1);
		});
	}

	@EnabledIf(TestAdhocIntegrationTests.ENABLED_IF)
	@Test
	public void testVariousQueries() {
		ICubeWrapper cube = editCube().engine(editEngine()
				.factories(
						makeFactories().toBuilder().stopwatchFactory(IStopwatchFactory.guavaStopwatchFactory()).build())
				.build()).build();

		CubeRelevancy relevancies = new RelevancyHeuristic()
				.computeRelevancies(MeasureForest.fromMeasures("unitTests", cube.getMeasures()));
		Set<String> columns = cube.getColumnsAsMap().keySet();

		List<Entry<String, MeasureRelevancy>> mostRelevantMeasures = relevancies.getMeasureToRelevancy()
				.entrySet()
				.stream()
				.sorted(Comparator.<Map.Entry<String, MeasureRelevancy>, Double>comparing(e -> e.getValue().getScore()))
				.limit(3)
				.toList();

		int maxNbGroupBy = 2;

		for (int nbColumns = 1; nbColumns <= Math.min(columns.size(), maxNbGroupBy); nbColumns++) {
			List<Set<String>> columnsToGroupBy = IntStream.range(0, nbColumns).mapToObj(i -> columns).toList();

			Set<List<String>> groupBys = Sets.cartesianProduct(columnsToGroupBy);
			log.info("Considering {} groupBys", groupBys.size());
			groupBys.forEach(columnsInGroupBy -> {
				ITabularView result = cube().execute(CubeQuery.builder()
						.groupBy(GroupByColumns.named(columnsInGroupBy))
						.measure("event_count")
						.build());
				MapBasedTabularView mapBased = MapBasedTabularView.load(result);
				Assertions.assertThat(mapBased.getCoordinatesToValues()).isNotEmpty();
			});
		}
	}

	// event_count relies on a Dispatchor
	@Test
	public void testFilterGoalsEventCount() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("event_count")
				.andFilter("event_code", "G")
				.andFilter("minute", 13)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("event_count", 17L).hasSize(1);
		});
	}

	// event_count relies on a Dispatchor, with a filter on a generated column
	@Test
	public void testFilterGoalsCount() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("goal_count").andFilter("minute", 13).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("goal_count", 17L).hasSize(1);
		});
	}

	// match_count relies on a Partitionor
	@Test
	public void testMatchCount() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("match_count").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("match_count", 836L).hasSize(1);
		});
	}

	@Test
	public void testMatchCount_previousWorldCup() {
		ITabularView result = cube().execute(CubeQuery.builder().measure("match_count.previousWorldCup").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("match_count.previousWorldCup", 836L).hasSize(1);
		});
	}

	@Test
	public void testMatchCount_previousWorldCup_groupByYear() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure("match_count", "match_count.previousWorldCup").groupByAlso("year").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(20)
				.hasEntrySatisfying(Map.of("year", 1998L), v -> {
					Assertions.assertThat((Map) v)
							.containsEntry("match_count", 64L)
							.containsEntry("match_count.previousWorldCup", 52L)
							.hasSize(2);
				});
	}

	@Test
	public void testMatchCount_sinceInception_grandTotal() {
		ITabularView result =
				cube().execute(CubeQuery.builder().measure("match_count", "match_count.sinceInception2").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 836L)
					.containsEntry("match_count.sinceInception2", 836L)
					.hasSize(2);
		});
	}

	@Test
	public void testMatchCount_sinceInception_filterYear() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("match_count", "match_count.sinceInception2")
				.andFilter("year", 1998L)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 64L)
					.containsEntry("match_count.sinceInception2", 580L)
					.hasSize(2);
		});
	}

	@Disabled("coach_score fails as match_count is meaningless groupedBy minute")
	@Test
	public void testCoachScore_byMinute() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("coach_score")
				.groupByAlso("minute")
				.filter(AndFilter.and(ColumnFilter.matchLike("Coach name", "LORENZO%"),
						ColumnFilter.matchEq("minute", 4)))
				.debug(true)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 64L)
					.containsEntry("match_count.sinceInception2", 580L)
					.hasSize(2);
		});
	}

	@Test
	public void testMatchCount_sinceInception_groupByYear() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure("match_count", "match_count.sinceInception2").groupByAlso("year").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("year", 1930L), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 18L)
					.containsEntry("match_count.sinceInception2", 18L)
					.hasSize(2);
		}).hasEntrySatisfying(Map.of("year", 1934L), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 17L)
					.containsEntry("match_count.sinceInception2", 18L + 17L)
					.hasSize(2);
		}).hasEntrySatisfying(Map.of("year", 1998L), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 64L)
					.containsEntry("match_count.sinceInception2", 580L)
					.hasSize(2);
		}).hasEntrySatisfying(Map.of("year", 2002L), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 64L)
					.containsEntry("match_count.sinceInception2", 580L + 64L)
					.hasSize(2);
		}).hasSize(20);
	}

	@Test
	public void testCoachScore_sinceInception_groupByYear() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure("coach_score", "coach_score.sinceInception2").groupByAlso("year").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("year", 1998L), v -> {
			Assertions.assertThat((Map) v).hasEntrySatisfying("coach_score", score -> {
				Assertions.assertThat(score).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isBetween(0.71, 0.72);
			}).hasEntrySatisfying("coach_score.sinceInception2", score -> {
				Assertions.assertThat(score).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isBetween(20.09, 20.10);
			}).hasSize(2);
		}).hasSize(20);
	}

	@Test
	public void testCoachScore_sinceInception_groupByYear_filterByYear() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("coach_score", "coach_score.sinceInception2")
				.groupByAlso("year")
				.andFilter("year", 1998L)
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("year", 1998L), v -> {
			Assertions.assertThat((Map) v).hasEntrySatisfying("coach_score", score -> {
				Assertions.assertThat(score).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isBetween(0.71, 0.72);
			}).hasEntrySatisfying("coach_score.sinceInception2", score -> {
				Assertions.assertThat(score).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isBetween(20.09, 20.10);
			}).hasSize(2);
		}).hasSize(1);
	}

	// This check calculatedStar over a generatedColumn
	@Test
	public void testEventCount_byMinute_withCalculatedStar() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("event_count")
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("minute")
						.calculatedCoordinate(CalculatedCoordinate.star())
						.build()))
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("minute", 1L), v -> {
			Assertions.assertThat(v).isEqualTo(Map.of("event_count", 135L));
		}).hasEntrySatisfying(Map.of("minute", 90L), v -> {
			Assertions.assertThat(v).isEqualTo(Map.of("event_count", 328L));
		}).hasEntrySatisfying(Map.of("minute", "*"), v -> {
			Assertions.assertThat(v).isEqualTo(Map.of("event_count", 11_270L));
		}).hasSize(120 + 1);
	}

	@Test
	public void testEventCount_byPosition_nullCoordinate() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("event_count")
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("Position")
						.calculatedCoordinate(CalculatedCoordinate.star())
						.build()))
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("Position", "GKC"), v -> {
			Assertions.assertThat(v).isEqualTo(Map.of("event_count", 13L));
		}).hasEntrySatisfying(MapWithNulls.of("Position", null), v -> {
			Assertions.assertThat(v).isEqualTo(Map.of("event_count", 10569L));
		}).hasSize(4 + 1);
	}

	@Test
	public void testEventCount_LIKE_AwayTeamGoals() {
		ITabularView result = cube().execute(CubeQuery.builder()
				.measure("event_count")
				.groupByAlso("Away Team Goals")
				.filter(ColumnFilter.matchLike("Away Team Goals", "%7%"))
				.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasEntrySatisfying(Map.of("Away Team Goals", 7L), v -> {
					Assertions.assertThat(v).isEqualTo(Map.of("event_count", 100L));
				})
				.hasSize(1);
	}

}
