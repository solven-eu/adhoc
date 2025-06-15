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
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
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
	public ICubeWrapper makeCube() {
		return CubeWrapper.builder()
				.name(worldCupSchema.getName())
				.forest(forest)
				.table(tableSupplier.get())
				.queryPreparator(GeneratedColumnsPreparator.builder().generatedColumnsMeasure("event_count").build())
				.build();
	}

	@Test
	public void testGrandTotal() {
		ITabularView result = cubeSupplier.get().execute(CubeQuery.builder().measure(countAsterisk.getName()).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry(countAsterisk.getName(), 37784L).hasSize(1);
		});
	}

	@Test
	public void testFilterGoalsEventCount() {
		ITabularView result = cubeSupplier.get()
				.execute(CubeQuery.builder()
						.measure("event_count")
						.andFilter("event_code", "G")
						.andFilter("minute", 13)
						.build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("event_count", 15L).hasSize(1);
		});
	}

	@Test
	public void testFilterGoalsCount() {
		ITabularView result =
				cubeSupplier.get().execute(CubeQuery.builder().measure("goal_count").andFilter("minute", 13).build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("goal_count", 15L).hasSize(1);
		});
	}

	@Test
	public void testMatchCount() {
		ITabularView result = cubeSupplier.get().execute(CubeQuery.builder().measure("match_count").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), v -> {
			Assertions.assertThat((Map) v).containsEntry("match_count", 836L).hasSize(1);
		});
	}

}
