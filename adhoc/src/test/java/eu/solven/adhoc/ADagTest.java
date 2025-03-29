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

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.UnsafeMeasureForestBag;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.util.IStopwatch;
import eu.solven.adhoc.util.IStopwatchFactory;

/**
 * Helps testing anything related with a {@link MeasureForest} or a {@link AdhocQueryEngine}
 * 
 * @author Benoit Lacelle
 *
 */
public abstract class ADagTest {
	public final EventBus eventBus = new EventBus();
	public final Object toSlf4j = new AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel();
	public final UnsafeMeasureForestBag amb =
			UnsafeMeasureForestBag.builder().name(this.getClass().getSimpleName()).build();

	public IStopwatch makeStopwatch() {
		return Stopwatch.createStarted()::elapsed;
	}

	public final IStopwatchFactory stopwatchFactory = new IStopwatchFactory() {

		@Override
		public IStopwatch createStarted() {
			return makeStopwatch();
		}
	};
	public final AdhocQueryEngine aqe =
			AdhocQueryEngine.builder().eventBus(eventBus::post).stopwatchFactory(stopwatchFactory).build();

	public final InMemoryTable rows = InMemoryTable.builder().build();
	public final Supplier<ITableWrapper> tableSupplier = Suppliers.memoize(this::makeTable);
	public final CubeWrapper aqw =
			CubeWrapper.builder().table(tableSupplier.get()).engine(aqe).forest(amb).eventBus(eventBus::post).build();

	public ITableWrapper makeTable() {
		return rows;
	}

	@BeforeEach
	public void wireEvents() {
		eventBus.register(toSlf4j);
	}

	// `@BeforeEach` has to be duplicated on each implementation
	// @BeforeEach
	public abstract void feedTable();

	/**
	 * Typically used to edit the operatorsFactory
	 */
	public AdhocQueryEngine.AdhocQueryEngineBuilder editEngine() {
		return aqe.toBuilder();
	}

	/**
	 * Typically used to edit the operatorsFactory
	 */
	public CubeWrapperBuilder editCube() {
		return aqw.toBuilder();
	}

}
