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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.column.generated_column.ColumnGeneratorHelpers;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.column.generated_column.IMayHaveColumnGenerator;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IHasOperatorFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IAdhocEventBus;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Combines an {@link CubeQueryEngine}, including its {@link MeasureForest} and a {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder(toBuilder = true)
@Slf4j
public class CubeWrapper implements ICubeWrapper {
	@NonNull
	@Builder.Default
	@Getter
	final String name = "someCubeName";

	// Execute the query
	@NonNull
	@Default
	final ICubeQueryEngine engine = CubeQueryEngine.builder().build();
	// Holds the data
	@NonNull
	final ITableWrapper table;
	// Holds the indicators definitions
	@NonNull
	final IMeasureForest forest;
	// Enable transcoding from table to cube
	@NonNull
	@Default
	final IColumnsManager columnsManager = ColumnsManager.builder().build();
	// Wrap a query (e.g. with queryId, implicitFilter, etc)
	@NonNull
	@Default
	final IQueryPreparator queryPreparator = StandardQueryPreparator.builder().build();

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return forest.getNameToMeasure();
	}

	@Override
	public ITabularView execute(ICubeQuery query) {
		return engine.execute(queryPreparator.prepareQuery(table, forest, columnsManager, query));
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		Map<String, ColumnMetadata> columnToType = getColumnsWithoutAliases();

		// Register aliases in the `alias` field of metadata
		// TODO This does not handle recursive aliases
		getColumnsManager().getColumnAliases().forEach(columnAlias -> {
			String tableName = getColumnsManager().openTranscodingContext().underlying(columnAlias);

			ColumnMetadata originalMetadata = columnToType.get(tableName);

			if (originalMetadata == null) {
				log.debug("Unclear alias=%s as it has no underlying column", columnAlias);
				columnToType.put(columnAlias,
						ColumnMetadata.builder().name(columnAlias).tag("alias").type(Object.class).build());
			} else {
				columnToType.put(originalMetadata.getName(), originalMetadata.toBuilder().alias(columnAlias).build());
			}
		});

		// Duplicate each column given its alias
		Map<String, ColumnMetadata> aliasToColumn = new LinkedHashMap<>();
		columnToType.forEach((column, metadata) -> {
			metadata.getAliases().forEach(alias -> {
				aliasToColumn.put(alias, metadata.toBuilder().name(alias).alias(column).build());
			});
		});

		columnToType.putAll(aliasToColumn);

		return columnToType.values();
	}

	private Map<String, ColumnMetadata> getColumnsWithoutAliases() {
		Map<String, ColumnMetadata> columnToType = new HashMap<>();

		// First, register table columns
		table.getColumns().forEach((table) -> {
			columnToType.put(table.getName(), table);
		});

		// Then, register calculated columns (e.g. based on an expression)
		getColumnsManager().getColumnTypes().forEach((columnName, type) -> {
			columnToType.put(columnName,
					ColumnMetadata.builder().name(columnName).tag("calculated").type(type).build());
		});

		IOperatorFactory operatorFactory = IHasOperatorFactory.getOperatorsFactory(engine);
		forest.getMeasures().forEach(measure -> {
			if (measure instanceof IMayHaveColumnGenerator mayHaveColumnGenerator) {
				try {

					Optional<IColumnGenerator> optColumnGenerator =
							mayHaveColumnGenerator.optColumnGenerator(operatorFactory);

					optColumnGenerator.ifPresent(columnGenerator -> {
						// TODO How conflicts should be handled?
						columnGenerator.getColumnTypes().forEach((columnName, type) -> {
							columnToType.put(columnName,
									ColumnMetadata.builder().name(columnName).tag("generated").type(type).build());
						});
					});
				} catch (Exception e) {
					if (AdhocUnsafe.isFailFast()) {
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
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class CubeWrapperBuilder {
		public CubeWrapperBuilder eventBus(IAdhocEventBus eventBus) {
			// BEWARE Is this the proper way the ensure the eventBus is written in proper places?
			ColumnsManager columnsManager = (ColumnsManager) this.build().getColumnsManager();
			this.columnsManager(columnsManager.toBuilder().eventBus(eventBus).build());

			return this;
		}
	}

	@Override
	public Map<String, CoordinatesSample> getCoordinates(Map<String, IValueMatcher> columnToValueMatcher, int limit) {
		IOperatorFactory operatorFactory = IHasOperatorFactory.getOperatorsFactory(engine);

		List<IColumnGenerator> columnGenerators = columnsManager.getGeneratedColumns(operatorFactory,
				forest.getMeasures(),
				InMatcher.matchIn(columnToValueMatcher.keySet()));

		Set<String> generatedColumns = columnGenerators.stream()
				.flatMap(cg -> cg.getColumnTypes().keySet().stream())
				.collect(Collectors.toSet());

		Map<String, CoordinatesSample> columnToSample = new TreeMap<>();

		// Given columns are defined by a measure, not by the table
		for (String generatedColumn : generatedColumns) {
			IValueMatcher valueMatcher = columnToValueMatcher.get(generatedColumn);
			CoordinatesSample coordinates =
					ColumnGeneratorHelpers.getCoordinates(columnGenerators, generatedColumn, valueMatcher, limit);
			columnToSample.put(generatedColumn, coordinates);
		}

		// TODO What if a column is both from table and generated? Trying to get coordinates from an
		// unknown column would generally lead to an error.
		{

			// Can not rely on `table.getColumns().containsKey(column)` as many table have various ways to match a
			// column
			// e.g. `someColumn` and `p.someColumn` may match the same column, while it is unclear to us how to return
			// `p.someColumn` as a column from JooQ
			Map<String, IValueMatcher> tableColumnToValueMatcher = new LinkedHashMap<>();

			AliasingContext transcodedContext = columnsManager.openTranscodingContext();
			Sets.difference(columnToValueMatcher.keySet(), generatedColumns).forEach(cubeColumn -> {
				String tableColumn = transcodedContext.underlyingNonNull(cubeColumn);
				tableColumnToValueMatcher.put(tableColumn, columnToValueMatcher.get(cubeColumn));
			});

			Map<String, CoordinatesSample> tableCoordinates = table.getCoordinates(tableColumnToValueMatcher, limit);

			tableCoordinates.forEach((tableColumn, sample) -> {
				transcodedContext.queried(tableColumn)
						.forEach(queriedColumn -> columnToSample.put(queriedColumn, sample));
			});
		}

		return columnToSample;
	}

}
