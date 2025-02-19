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

import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.cube.AdhocCubeWrapper.AdhocCubeWrapperBuilder;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.InMemoryTable;

/**
 * Helps testing anything related with a {@link AdhocMeasureBag} or a {@link AdhocQueryEngine}
 * 
 * @author Benoit Lacelle
 *
 */
public abstract class ADagTest {
	public final EventBus eventBus = new EventBus();
	public final Object toSlf4j = new AdhocEventsFromGuavaEventBusToSfl4j_DebugLevel();
	public final AdhocMeasureBag amb = AdhocMeasureBag.builder().name(this.getClass().getSimpleName()).build();
	public final AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(eventBus::post).build();

	public final InMemoryTable rows = InMemoryTable.builder().build();
	public final Supplier<IAdhocTableWrapper> tableSupplier = Suppliers.memoize(this::makeTable);
	public final AdhocCubeWrapper aqw = AdhocCubeWrapper.builder()
			.table(tableSupplier.get())
			.engine(aqe)
			.measures(amb)
			.eventBus(eventBus::post)
			.build();

	public IAdhocTableWrapper makeTable() {
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
		return AdhocQueryEngine.edit(aqe);
	}

	/**
	 * Typically used to edit the operatorsFactory
	 */
	public AdhocCubeWrapperBuilder editCube() {
		return AdhocCubeWrapper.edit(aqw);
	}

}
