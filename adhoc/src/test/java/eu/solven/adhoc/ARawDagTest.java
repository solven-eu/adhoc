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
package eu.solven.adhoc;

import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;

import com.google.common.base.Suppliers;
import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.composite.StopWatchTestFactory;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.IStopwatchFactory;

/**
 * Helps testing anything related with a {@link MeasureForest} or a {@link CubeQueryEngine}
 * 
 * @author Benoit Lacelle
 *
 */
public abstract class ARawDagTest {
	public final EventBus eventBus = AdhocTestHelper.eventBus();
	public final UnsafeMeasureForest forest =
			UnsafeMeasureForest.builder().name(this.getClass().getSimpleName()).build();

	public IStopwatch makeStopwatch() {
		return stopwatchFactory.createStarted();
	}

	public IStopwatchFactory makeStopwatchFactory() {
		return new StopWatchTestFactory();
	}

	public final IStopwatchFactory stopwatchFactory = makeStopwatchFactory();

	public AdhocFactories makeFactories() {
		return AdhocFactories.builder().stopwatchFactory(stopwatchFactory).build();
	}

	public CubeQueryEngine engine() {
		return CubeQueryEngine.builder().eventBus(eventBus::post).factories(makeFactories()).build();
	}

	public abstract ITableWrapper makeTable();

	public final Supplier<ITableWrapper> tableSupplier = Suppliers.memoize(this::makeTable);

	public ITableWrapper table() {
		return tableSupplier.get();
	}

	public CubeWrapper.CubeWrapperBuilder makeCube() {
		return CubeWrapper.builder()
				.table(table())
				.engine(engine())
				.forest(forest)
				.eventBus(eventBus::post)
				.queryPreparator(queryPreparator())
		// .columnsManager(ColumnsManager.builder().transcoder(transcoder).build())
		;
	}

	protected IQueryPreparator queryPreparator() {
		return StandardQueryPreparator.builder().build();
	}

	public final Supplier<ICubeWrapper> cubeSupplier = Suppliers.memoize(() -> makeCube().build());

	public ICubeWrapper cube() {
		return cubeSupplier.get();
	}

	@BeforeEach
	public void wireEvents() {
		// AdhocTestHelper.eventBus registered an InfoLevel
		// eventBus.register(new AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel());
	}

	/**
	 * Typically used to edit the operatorFactory
	 */
	public CubeQueryEngine.CubeQueryEngineBuilder editEngine() {
		return engine().toBuilder();
	}

	/**
	 * Typically used to edit the operatorFactory
	 */
	public CubeWrapperBuilder editCube() {
		return ((CubeWrapper) cube()).toBuilder();
	}

}
