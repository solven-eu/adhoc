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
package eu.solven.adhoc.cube;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.column.IAdhocColumnsManager;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IAdhocMeasureBag;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 * Combines an {@link AdhocQueryEngine}, including its {@link AdhocMeasureBag} and a {@link IAdhocTableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocCubeWrapper implements IAdhocCubeWrapper {
	@NonNull
	@Builder.Default
	@Getter
	final String name = "someCubeName";

	@NonNull
	@Default
	final IAdhocQueryEngine engine = AdhocQueryEngine.builder().build();
	@NonNull
	final IAdhocMeasureBag measures;
	@NonNull
	final IAdhocTableWrapper table;
	@NonNull
	@Default
	final IAdhocColumnsManager columnsManager = AdhocColumnsManager.builder().build();
	
	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return measures.getNameToMeasure();
	}

	@Override
	public ITabularView execute(IAdhocQuery query, Set<? extends IQueryOption> options) {
		return engine.execute(query, options, measures, table, columnsManager);
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columnToType = new HashMap<>();

		// First we register table columns
		columnToType.putAll(table.getColumns());

		return columnToType;
	}

	/**
	 * Typically useful when the {@link IAdhocTableWrapper} has to be changed, but the other parameters must be kept.
	 *
	 * @param template
	 *            some template
	 * @return
	 */
	public static AdhocCubeWrapperBuilder edit(AdhocCubeWrapper template) {
		return AdhocCubeWrapper.builder().engine(template.engine).measures(template.measures).table(template.table);
	}

	public static class AdhocCubeWrapperBuilder {
		public AdhocCubeWrapperBuilder eventBus(IAdhocEventBus eventBus) {
			AdhocColumnsManager columnsManager = (AdhocColumnsManager) this.build().getColumnsManager();
			this.columnsManager(columnsManager.toBuilder().eventBus(eventBus).build());

			return this;
		}
	}

}
