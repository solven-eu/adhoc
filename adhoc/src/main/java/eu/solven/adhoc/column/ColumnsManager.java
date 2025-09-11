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
package eu.solven.adhoc.column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.generated_column.ColumnGeneratorHelpers;
import eu.solven.adhoc.column.generated_column.EmptyColumnGenerator;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.tabular.AdhocExceptionAsMeasureValueHelper;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.IHasAliasedColumns;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.ITableReverseAliaser;
import eu.solven.adhoc.table.transcoder.IdentityImplicitAliaser;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import eu.solven.adhoc.table.transcoder.value.StandardCustomTypeManager;
import eu.solven.adhoc.util.AdhocBlackHole;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

/**
 * Default {@link IColumnsManager}.
 * 
 * @author Benoit Lacelle
 */
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
@Slf4j
public class ColumnsManager implements IColumnsManager {

	@NonNull
	@Default
	final IAdhocEventBus eventBus = AdhocBlackHole.getInstance();

	@Default
	@NonNull
	@Getter
	final ITableAliaser aliaser = new IdentityImplicitAliaser();

	@NonNull
	@Default
	final IMissingColumnManager missingColumnManager = StandardMissingColumnManager.builder().build();

	@NonNull
	@Default
	final ICustomTypeManager customTypeManager = new StandardCustomTypeManager();

	@NonNull
	@Singular
	final ImmutableSet<ICalculatedColumn> calculatedColumns;

	@Default
	@NonNull
	final IColumnGenerator columnGenerator = EmptyColumnGenerator.empty();

	@Override
	public ITabularRecordStream openTableStream(QueryPod queryPod, TableQueryV2 query) {
		AliasingContext transcodingContext = openTranscodingContext();

		ISliceFilter transcodedFilter;
		{
			ISliceFilter notTranscodedFilter = query.getFilter();

			Set<String> calculatedColumns = getFiltrableCalculatedColumns(query);
			Set<String> calculatedAndFiltered =
					Sets.intersection(calculatedColumns, FilterHelpers.getFilteredColumns(notTranscodedFilter));
			if (!calculatedAndFiltered.isEmpty()) {
				throw new NotYetImplementedException(
						"Can not filter along calculated columns: %s".formatted(calculatedAndFiltered));
			}

			transcodedFilter = transcodeFilter(transcodingContext, notTranscodedFilter);

			// Sanity checks
			FilterHelpers.getFilteredColumns(transcodedFilter).forEach(underlying -> {
				Set<String> queried = transcodingContext.queried(underlying);
				if (queried.size() >= 2) {
					eventBus.post(AdhocLogEvent.builder()
							.warn(true)
							.message("Ambiguous filtered column: %s -> %s (filter=%s)"
									.formatted(underlying, queried, notTranscodedFilter)));
				}
			});
		}
		TableQueryV2 transcodedQuery = query.toBuilder()
				.filter(transcodedFilter)
				.groupBy(transcodeGroupBy(transcodingContext, query.getGroupBy()))
				.clearAggregators()
				.aggregators(transcodeAggregators(transcodingContext, query.getAggregators()))
				.build();

		if (queryPod.isDebug()) {
			eventBus.post(AdhocLogEvent.builder()
					.debug(true)
					.message("Transcoded query is `%s` given `%s`".formatted(transcodedQuery, query))
					.source(this)
					.build());
		}
		if (queryPod.isExplain() && !transcodingContext.isOnlyIdentity()) {
			eventBus.post(AdhocLogEvent.builder()
					.explain(true)
					.message("Transcoded context is %s".formatted(transcodingContext))
					.source(this)
					.build());
		}

		ITableWrapper table = queryPod.getTable();
		ITabularRecordStream tabularRecordStream;

		try {
			tabularRecordStream = table.streamSlices(queryPod, transcodedQuery);
		} catch (RuntimeException e) {
			if (queryPod.getOptions().contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)) {
				tabularRecordStream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(transcodedQuery, e);
			} else {
				String msgE = "Issue opening stream from %s for query=%s".formatted(table, transcodedQuery);
				throw AdhocExceptionHelpers.wrap(e, msgE);
			}
		}

		return transcodeRows(transcodingContext, tabularRecordStream, transcodedFilter);
	}

	protected Set<String> getFiltrableCalculatedColumns(TableQueryV2 query) {
		Set<String> calculatedColumns = new TreeSet<>();
		this.calculatedColumns.forEach(calculatedColumn -> calculatedColumns.add(calculatedColumn.getName()));
		query.getGroupBy()
				.getNameToColumn()
				.values()
				.stream()
				.filter(c -> c instanceof ICalculatedColumn
						&& !(c instanceof FunctionCalculatedColumn f && f.isSkipFiltering()))
				.forEach(calculatedColumn -> calculatedColumns.add(calculatedColumn.getName()));
		return calculatedColumns;
	}

	protected ITabularRecordStream transcodeRows(AliasingContext transcodingContext,
			ITabularRecordStream tabularRecordStream,
			ISliceFilter transcodedFilter) {
		return new ITabularRecordStream() {

			@Override
			public boolean isDistinctSlices() {
				// TODO Study how this flag could be impacted by transcoding
				if (transcodingContext.getNameToCalculated().isEmpty()) {
					return tabularRecordStream.isDistinctSlices();
				} else {
					// TODO Investigate deeper this case
					// But a calculated column could lead to additional groupBys. Hence, we may receive multiple entries
					// for a slice given columns of the original query
					return false;
				}
			}

			@Override
			public void close() {
				tabularRecordStream.close();
			}

			@Override
			public Stream<ITabularRecord> records() {
				IColumnValueTranscoder valueTranscoder = prepareTypeTranscoder(transcodingContext);
				ITableReverseAliaser columnTranscoder = prepareColumnTranscoder(transcodingContext);

				return tabularRecordStream.records()
						.map(row -> transcodeTypes(valueTranscoder, row))
						// TODO Should we transcode type before or after columnNames?
						.map(notTranscoded -> notTranscoded.transcode(columnTranscoder))
						// calculate columns after transcoding, as these expression are generally table-independant
						.map(row -> evaluateCalculated(transcodingContext, row))
				// TODO Filter
				// .filter(row -> filterCalculatedColumns(transcodedFilter, row))
				;
			}

			@Override
			public String toString() {
				return "Transcoding: " + tabularRecordStream;
			}
		};
	}

	// protected boolean filterCalculatedColumns(IAdhocFilter transcodedFilter, ITabularRecord row) {
	// return true;
	// }

	protected ITableReverseAliaser prepareColumnTranscoder(AliasingContext transcodingContext) {
		int estimatedSize = transcodingContext.estimateQueriedSize(transcodingContext.underlyings());
		return new ITableReverseAliaser() {

			@Override
			public Set<String> queried(String underlying) {
				return transcodingContext.queried(underlying);
			}

			@Override
			public int estimateQueriedSize(Set<String> underlyingKeys) {
				return estimatedSize;
			}
		};
	}

	protected ITabularRecord evaluateCalculated(AliasingContext transcodingContext, ITabularRecord row) {
		Map<String, FunctionCalculatedColumn> columns = transcodingContext.getNameToCalculated();

		if (columns.isEmpty()) {
			return row;
		}

		Map<String, Object> enrichedGroupBy = new LinkedHashMap<>(row.getGroupBys().getCoordinates());

		columns.forEach((columnName, column) -> {
			// TODO handle recursive formulas (e.g. a formula relying on another formula)
			enrichedGroupBy.put(columnName, column.computeCoordinate(row));
		});

		return TabularRecordOverMaps.builder()
				.slice(SliceAsMap.fromMap(enrichedGroupBy))
				.aggregates(row.aggregatesAsMap())
				.build();
	}

	protected IColumnValueTranscoder prepareTypeTranscoder(AliasingContext transcodingContext) {
		Set<String> mayBeTypeTranscoded = transcodingContext.underlyings()
				.stream()
				.filter(customTypeManager::mayTranscode)
				.collect(Collectors.toSet());

		IColumnValueTranscoder valueTranscoder = new IColumnValueTranscoder() {

			@Override
			public Set<String> mayTranscode(Set<String> recordColumns) {
				return Sets.intersection(mayBeTypeTranscoded, recordColumns);
			}

			@Override
			public Object transcodeValue(String column, Object value) {
				return customTypeManager.fromTable(column, value);
			}
		};
		return valueTranscoder;
	}

	protected ITabularRecord transcodeTypes(IColumnValueTranscoder valueTranscoder, ITabularRecord row) {
		return row.transcode(valueTranscoder);
	}

	@Override
	public AliasingContext openTranscodingContext() {
		return AliasingContext.builder().aliaser(getAliaser()).build();
	}

	protected ISliceFilter transcodeFilter(ITableAliaser tableTranscoder, ISliceFilter filter) {
		return MoreFilterHelpers.transcodeFilter(customTypeManager, tableTranscoder, filter);
	}

	protected IAdhocGroupBy transcodeGroupBy(AliasingContext aliasingContext, IAdhocGroupBy groupBy) {
		NavigableMap<String, IAdhocColumn> nameToColumn = groupBy.getNameToColumn();

		List<IAdhocColumn> transcoded = nameToColumn.values()
				.stream()
				// Replace a reference column by a calculated column (if applicable)
				.map(c -> {
					if (c instanceof ReferencedColumn referencedColumn) {
						String columnName = referencedColumn.getName();
						Optional<ICalculatedColumn> calculatedColumn = calculatedColumns.stream()
								.filter(calculated -> calculated.getName().equals(columnName))
								.findFirst();
						if (calculatedColumn.isPresent()) {
							return calculatedColumn.get();
						}
					}
					return c;
				})
				// flatMap to the underlying columns
				.flatMap(c -> {
					if (c instanceof ReferencedColumn referencedColumn) {
						String columnName = referencedColumn.getName();
						return Stream.of(aliasingContext.underlying(columnName)).map(ReferencedColumn::ref);
					} else if (c instanceof FunctionCalculatedColumn calculatedColumn) {
						aliasingContext.addCalculatedColumn(calculatedColumn);

						Collection<ReferencedColumn> operandColumns =
								FunctionCalculatedColumn.getUnderlyingColumns(calculatedColumn);
						return operandColumns.stream()
								.map(operandColumn -> aliasingContext.underlying(operandColumn.getName()))
								.map(ReferencedColumn::ref);
					} else if (c instanceof TableExpressionColumn expressionColumn) {
						aliasingContext.underlying(expressionColumn.getName());

						// BEWARE To handle transcoding, one would need to parse the SQL, to replace columns references
						eventBus.post(AdhocLogEvent.builder()
								.warn(true)
								.message("BEWARE If %s should be impacted by transcoding".formatted(expressionColumn))
								.source(this)
								.build());
						return Stream.of(expressionColumn);
					} else {
						throw new UnsupportedOperationException(
								"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(c)));
					}
				})
				.toList();

		return GroupByColumns.of(transcoded);
	}

	protected Collection<? extends FilteredAggregator> transcodeAggregators(AliasingContext transcodingContext,
			Set<FilteredAggregator> aggregators) {
		return aggregators.stream().map(filteredAggregator -> {
			Aggregator aggregator = filteredAggregator.getAggregator();
			Aggregator transcodedAggregator = Aggregator.edit(aggregator)
					.columnName(transcodingContext.underlying(aggregator.getColumnName()))
					.build();
			return filteredAggregator.toBuilder()
					.aggregator(transcodedAggregator)
					.filter(transcodeFilter(transcodingContext, filteredAggregator.getFilter()))
					.build();
		}).collect(Collectors.toSet());
	}

	@Override
	public Object onMissingColumn(String column) {
		return missingColumnManager.onMissingColumn(column);
	}

	@Override
	public Object onMissingColumn(ICubeWrapper cube, String column) {
		return missingColumnManager.onMissingColumn(cube, column);
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		Map<String, Class<?>> columnToType = new LinkedHashMap<>();

		// BEWARE What if they is conflicts? Should pick the higher type? (i.e. potential fallback to Object)
		calculatedColumns.forEach(c -> columnToType.put(c.getName(), c.getType()));
		columnToType.putAll(customTypeManager.getColumnTypes());
		columnToType.putAll(columnGenerator.getColumnTypes());

		return ImmutableMap.copyOf(columnToType);
	}

	@Override
	public List<IColumnGenerator> getGeneratedColumns(IOperatorFactory operatorFactory,
			Set<IMeasure> measures,
			IValueMatcher columnMatcher) {
		List<IColumnGenerator> columnGenerators = new ArrayList<>();

		columnGenerators.add(columnGenerator);
		columnGenerators.addAll(ColumnGeneratorHelpers.getColumnGenerators(operatorFactory, measures, columnMatcher));

		return columnGenerators;
	}

	@Override
	public Set<String> getColumnAliases() {
		if (aliaser instanceof IHasAliasedColumns hasAliasedColumns) {
			return hasAliasedColumns.getAlias();
		} else {
			return Set.of();
		}
	}

}
