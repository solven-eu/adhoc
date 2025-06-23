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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.context.GeneratedColumnsPreparator;
import eu.solven.adhoc.example.worldcup.WorldCupPlayersSchema;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.duckdb.DuckDbHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * 
 * @author Benoit Lacelle
 * @see InjectWorldCupExampleCubesConfig
 */
@Slf4j
public class TestTableQuery_DuckDb_WorldCup extends ARawDagTest implements IAdhocTestConstants {

	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	DSLSupplier dslSupplier = DuckDbHelper.inMemoryDSLSupplier();

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

	@Test
	public void testMatchCount_sinceInception_groupByYear() {
		ITabularView result = cube().execute(
				CubeQuery.builder().measure("match_count", "match_count.sinceInception2").groupByAlso("year").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasEntrySatisfying(Map.of("year", 1998L), v -> {
			Assertions.assertThat((Map) v)
					.containsEntry("match_count", 64L)
					.containsEntry("match_count.sinceInception2", 580L)
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

}
