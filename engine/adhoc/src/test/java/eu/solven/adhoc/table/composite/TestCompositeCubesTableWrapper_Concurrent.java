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
package eu.solven.adhoc.table.composite;

import java.util.Map;
import java.util.concurrent.Phaser;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.composite.PhasedTableWrapper.TableWrapperPhasers;

public class TestCompositeCubesTableWrapper_Concurrent extends ARawDagTest implements IAdhocTestConstants {

	// Not used
	@Override
	public ITableWrapper makeTable() {
		return InMemoryTable.builder().build();
	}

	private CubeWrapper wrapInCube(IMeasureForest forest, ITableWrapper table) {
		return CubeWrapper.builder()
				.name(table.getName() + ".cube")
				.engine(engine())
				.forest(forest)
				.table(table)
				.build();
	}

	private CubeWrapper makeComposite(CompositeCubesTableWrapper compositeTable, IMeasureForest forest) {
		return CubeWrapper.builder().name("composite").table(compositeTable).forest(forest).engine(engine()).build();
	}

	// This test will ensure 2 underlying tables are queried concurrently
	@Test
	public void testConcurrency() {

		TableWrapperPhasers phasers = TableWrapperPhasers.parties(2);

		String tableName1 = "someTableName1";
		ITableWrapper table1 = PhasedTableWrapper.builder().name(tableName1).phasers(phasers).build();

		String tableName2 = "someTableName2";
		ITableWrapper table2 = PhasedTableWrapper.builder().name(tableName2).phasers(phasers).build();

		CubeWrapper cube1;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName1).build();
			measureBag.addMeasure(countAsterisk);
			cube1 = wrapInCube(measureBag, table1);
		}
		CubeWrapper cube2;
		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name(tableName2).build();
			measureBag.addMeasure(countAsterisk);
			cube2 = wrapInCube(measureBag, table2);
		}

		CompositeCubesTableWrapper compositeCubesTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		CubeWrapper cube = makeComposite(compositeCubesTable, forest);
		ITabularView view = cube.execute(CubeQuery.builder()
				.measure(Aggregator.countAsterisk())
				.option(StandardQueryOptions.CONCURRENT)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 2))
				.hasSize(1);

		Phaser opening = phasers.getOpening();
		Assertions.assertThat(opening.getPhase()).isEqualTo(1);
		Assertions.assertThat(opening.getArrivedParties()).isEqualTo(0);

		Phaser streaming = phasers.getStreaming();
		Assertions.assertThat(streaming.getPhase()).isEqualTo(1);
		Assertions.assertThat(streaming.getArrivedParties()).isEqualTo(0);

		Phaser closing = phasers.getClosing();
		Assertions.assertThat(closing.getPhase()).isEqualTo(1);
		Assertions.assertThat(closing.getArrivedParties()).isEqualTo(0);
	}

}
