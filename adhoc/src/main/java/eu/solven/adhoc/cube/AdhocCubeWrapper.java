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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.column.IAdhocColumnsManager;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.dag.DefaultQueryPreparator;
import eu.solven.adhoc.dag.IAdhocQueryEngine;
import eu.solven.adhoc.dag.IQueryPreparator;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.measure.IHasOperatorsFactory;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.column_generator.IColumnGenerator;
import eu.solven.adhoc.measure.transformator.column_generator.IMayHaveColumnGenerator;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Combines an {@link AdhocQueryEngine}, including its {@link MeasureForest} and a {@link IAdhocTableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder(toBuilder = true)
@Slf4j
public class AdhocCubeWrapper implements IAdhocCubeWrapper {
	@NonNull
	@Builder.Default
	@Getter
	final String name = "someCubeName";

	// Execute the query
	@NonNull
	@Default
	final IAdhocQueryEngine engine = AdhocQueryEngine.builder().build();
	// Holds the data
	@NonNull
	final IAdhocTableWrapper table;
	// Holds the indicators definitions
	@NonNull
	final IMeasureForest forest;
	// Enable transcoding from table to cube
	@NonNull
	@Default
	final IAdhocColumnsManager columnsManager = AdhocColumnsManager.builder().build();
	// Wrap a query (e.g. with queryId, implicitFilter, etc)
	@NonNull
	@Default
	final IQueryPreparator queryPreparator = DefaultQueryPreparator.builder().build();

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return forest.getNameToMeasure();
	}

	@Override
	public ITabularView execute(IAdhocQuery query, Set<? extends IQueryOption> options) {
		return engine.execute(queryPreparator.prepareQuery(table, forest, columnsManager, query, options));
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columnToType = new HashMap<>();

		// First we register table columns
		table.getColumns().forEach((column, type) -> {
			String tableColumn = columnsManager.transcodeToTable(column);

			columnToType.put(tableColumn, type);
		});

		IOperatorsFactory operatorsFactory = IHasOperatorsFactory.getOperatorsFactory(engine);
		forest.getMeasures().forEach(measure -> {
			if (measure instanceof IMayHaveColumnGenerator mayHaveColumnGenerator) {
				try {

					Optional<IColumnGenerator> optColumnGenerator =
							mayHaveColumnGenerator.optColumnGenerator(operatorsFactory);

					optColumnGenerator.ifPresent(columnGenerator -> {
						// TODO How conflicts should be handled?
						columnToType.putAll(columnGenerator.getColumns());
					});
				} catch (Exception e) {
					if (AdhocUnsafe.failFast) {
						String msg = "Issue looking for an %s in m=%s c=%s"
								.formatted(IColumnGenerator.class.getSimpleName(), measure, this.getName());
						throw new IllegalStateException(msg, e);
					} else {
						log.warn("Issue looking for an {} in m={} c={}",
								IColumnGenerator.class.getSimpleName(),
								measure,
								this.getName(),
								e);
					}
				}
			}
		});

		return columnToType;
	}

	/**
	 * Typically useful when the {@link IAdhocTableWrapper} has to be changed, but the other parameters must be kept.
	 * Another typical case is editing the measures.
	 *
	 * @param template
	 *            some template
	 * @return
	 */
	// public static AdhocCubeWrapperBuilder edit(AdhocCubeWrapper template) {
	// return AdhocCubeWrapper.builder().engine(template.engine).measures(template.measures).table(template.table);
	// }

	public static class AdhocCubeWrapperBuilder {
		public AdhocCubeWrapperBuilder eventBus(IAdhocEventBus eventBus) {
			// BEWARE Is this the proper way the ensure the eventBus is written in proper places?
			AdhocColumnsManager columnsManager = (AdhocColumnsManager) this.build().getColumnsManager();
			this.columnsManager(columnsManager.toBuilder().eventBus(eventBus).build());

			return this;
		}
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		IOperatorsFactory operatorsFactory = ((AdhocQueryEngine) engine).getOperatorsFactory();

		List<IColumnGenerator> columnGenerators =
				IColumnGenerator.getColumns(operatorsFactory, forest.getMeasures(), column);
		if (!columnGenerators.isEmpty()) {
			// Given column is actually defined by a measure, not by the table
			return IColumnGenerator.getCoordinates(columnGenerators, column, valueMatcher, limit);
		} else {
			return table.getCoordinates(column, valueMatcher, limit);
		}
	}

}
