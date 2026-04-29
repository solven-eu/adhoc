/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ARawDagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.encoding.page.ScopedValueAppendableTableFactory;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.map.factory.ColumnSliceFactory;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;

/**
 * Smoke test: end-to-end query execution with the {@link ScopedValueAppendableTableFactory} wired into the engine via
 * {@link AdhocFactories#getSliceFactoryFactory()}. Verifies that the scope is correctly established at every
 * virtual-thread dispatch site reached by a {@code CONCURRENT} composite query over two sub-cubes (both the composite's
 * own post-processing AND each sub-cube's engine re-entry).
 *
 * Replicates the scenario of {@code TestCompositeCubesTableWrapper_Concurrent#testConcurrency} with a scoped slice
 * factory; any missed dispatch site would surface as a {@link java.util.NoSuchElementException} from
 * {@code ScopedValue.get()}.
 */
public class TestCompositeCubesTableWrapper_ScopedValue extends ARawDagTest implements IAdhocTestConstants {

	// Not used — both sub-cubes build their own InMemoryTable.
	@Override
	public ITableWrapper makeTable() {
		return InMemoryTable.builder().build();
	}

	// Override the slice-factory supplier so both the top-level composite cube and each sub-cube pick up
	// ColumnSliceFactory backed by ScopedValueAppendableTable.
	@Override
	public AdhocFactories makeFactories() {
		return AdhocFactories.builder()
				.stopwatchFactory(stopwatchFactory)
				.sliceFactoryFactory(options -> ColumnSliceFactory.builder()
						.options(options)
						.appendableTableFactory(new ScopedValueAppendableTableFactory())
						.build())
				.build();
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

	@Test
	public void testCompositeConcurrent_count() {
		String tableName1 = "someTableName1";
		InMemoryTable table1 = InMemoryTable.builder().name(tableName1).build();
		table1.add(Map.of("k1", 123));
		table1.add(Map.of("k1", 234));

		String tableName2 = "someTableName2";
		InMemoryTable table2 = InMemoryTable.builder().name(tableName2).build();
		table2.add(Map.of("k1", 345));

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

		CompositeCubesTableWrapper compositeTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		CubeWrapper cube = makeComposite(compositeTable, forest);
		ITabularView view = cube.execute(CubeQuery.builder()
				.measure(Aggregator.countAsterisk())
				.option(StandardQueryOptions.CONCURRENT)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 2 + 1))
				.hasSize(1);
	}
}
