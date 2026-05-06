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

import org.jspecify.annotations.Nullable;
import org.slf4j.event.Level;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.calculated.ICalculatedColumn;
import eu.solven.adhoc.column.generated_column.ColumnGeneratorHelpers;
import eu.solven.adhoc.column.generated_column.EmptyColumnGenerator;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.cuboid.tabular.ITabularGroupByRecord;
import eu.solven.adhoc.dataframe.filter.FilterMatcher;
import eu.solven.adhoc.dataframe.filter.MoreFilterHelpers;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.dataframe.row.TabularRecordOverMaps;
import eu.solven.adhoc.engine.tabular.AdhocExceptionAsMeasureValueHelper;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.eventbus.UnsafeAdhocEventBusHelpers;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.AdhocBlackHole;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.model.column.FunctionCalculatedColumn;
import eu.solven.adhoc.model.column.IAdhocColumn;
import eu.solven.adhoc.model.column.ReferencedColumn;
import eu.solven.adhoc.model.column.TableExpressionColumn;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.query.IGroupBy;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.model.query.groupby.GroupByColumns.GroupByColumnsBuilder;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.IQueryPod;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.IHasAliasedColumns;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.ITableReverseAliaser;
import eu.solven.adhoc.table.transcoder.IdentityImplicitAliaser;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import eu.solven.adhoc.table.transcoder.value.StandardCustomTypeManager;
import eu.solven.adhoc.util.IHasName;
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
	final IAdhocEventBus eventBus = UnsafeAdhocEventBusHelpers.safeWrapper(AdhocBlackHole.getInstance());

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

	// TODO Unify `columnGenerator` with `calculatedColumns`: an IColumnGenerator registered here overlaps with the
	// ICalculatedColumn mechanism (both advertise column names + types and get merged into `cube.getColumns()`).
	// Ideally, IColumnGenerator-registered columns should be handled through the calculatedColumns path so there is
	// a single surface for manually-declared columns.
	@Default
	@NonNull
	final IColumnGenerator columnGenerator = EmptyColumnGenerator.empty();

	@Override
	public ITabularRecordStream openSlicesStream(IQueryPod queryPod, TableQueryV4 query) {
		return openStreamInternal(queryPod, query, false);
	}

	@Override
	public ITabularRecordStream openRowsStream(IQueryPod queryPod, TableQueryV3 query) {
		// Reuse the V4 transcoding pipeline (filter rewrite, missing-column handling, post-filter) by going
		// through V3.toV4() then converting back to V3 right before calling table.streamRows. Lossless because
		// the merger emits a V3-shaped query (single groupBy, single aggregator set) and `transcodeQuery`
		// preserves that shape.
		return openStreamInternal(queryPod, query.toV4(), true);
	}

	protected ITabularRecordStream openStreamInternal(IQueryPod queryPod, TableQueryV4 query, boolean isDT) {
		AliasingContext transcodingContext = openTranscodingContext();

		ISliceFilter transcodedFilter;
		ISliceFilter nonPushdownFilter;

		Set<String> nonPushdownColumns;
		{
			ISliceFilter notTranscodedFilter = query.getFilter();

			Set<String> calculatedColumns = getFiltrableCalculatedColumns(query);

			// Exclude the calculatedColumns as they can not be evaluated by the ITableWrapper
			// BEWARE Optimization is skipped as we expect low amount of optimizations, and it may be costly to
			// re-optimize in case of large `OR` (e.g. TableQueryOptimizeSingleAggregator)
			ISliceFilter pushedDown =
					SimpleFilterEditor.suppressColumn(notTranscodedFilter, calculatedColumns, Optional.empty());

			// We'll have to filter manually the rows given the calculated columns
			// BEWARE This may rely on standard columns, for filters like `custom=c1&standard=s1|custom=c2&standard=s2`
			nonPushdownFilter = FilterHelpers.stripWhereFromFilter(pushedDown, notTranscodedFilter);
			nonPushdownColumns = FilterHelpers.getFilteredColumns(nonPushdownFilter);

			transcodedFilter = transcodeFilter(transcodingContext, pushedDown);

			// Sanity checks
			FilterHelpers.getFilteredColumns(transcodedFilter).forEach(underlying -> {
				Set<String> queried = transcodingContext.queried(underlying);
				if (queried.size() >= 2) {
					UnsafeAdhocEventBusHelpers.logForkEventBus(eventBus,
							AdhocLogEvent.builder()
									.level(Level.WARN)
									.messageT("Ambiguous filtered column: %s -> %s (filter=%s)",
											underlying,
											queried,
											notTranscodedFilter)
									.build());
				}
			});
		}

		TranscodedResult transcoded = transcodeQuery(query, transcodingContext, transcodedFilter, nonPushdownColumns);
		TableQueryV4 transcodedQuery = transcoded.getTranscodedQuery();

		if (queryPod.isDebug()) {
			eventBus.post(AdhocLogEvent.builder()
					.debug(true)
					.messageT("Transcoded query is `%s` given `%s`", transcodedQuery, query)
					.source(this)
					.build());
		}
		if (queryPod.isExplain() && !transcodingContext.isIdentity()) {
			eventBus.post(AdhocLogEvent.builder()
					.explain(true)
					.messageT("Transcoded context is %s", transcodingContext)
					.source(this)
					.build());
		}

		ITableWrapper table = queryPod.getTable();
		ITabularRecordStream tabularRecordStream;

		try {
			if (isDT) {
				// TODO Clarify if `asCoveringV3` is always a perfect match, as in DT, inputV4 is always equivalent to a
				// V3
				tabularRecordStream = table.streamRows(queryPod, transcodedQuery.asCoveringV3());
			} else {
				tabularRecordStream = table.streamSlices(queryPod, transcodedQuery);
			}
		} catch (RuntimeException e) {
			if (StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE.isActive(queryPod.getOptions())) {
				tabularRecordStream = AdhocExceptionAsMeasureValueHelper.makeErrorStream(transcodedQuery, e);
			} else {
				String msgE = "Issue opening stream from %s for query=%s".formatted(table, transcodedQuery);
				throw AdhocExceptionHelpers.wrap(msgE, e);
			}
		}

		return transcodeRows(transcodingContext, tabularRecordStream, nonPushdownFilter, transcoded);
	}

	/**
	 * Transcodes the query and computes the per-grouping-set {@code transcodedToOriginal} map alongside.
	 * {@code transcodingContext} is mutated (its calculated-column registry is populated by {@link #transcodeGroupBy}).
	 */
	protected TranscodedResult transcodeQuery(TableQueryV4 query,
			AliasingContext transcodingContext,
			ISliceFilter transcodedFilter,
			Set<String> nonPushdownColumns) {
		TableQueryV4.TableQueryV4Builder transcodedQueryBuilder =
				query.toBuilder().filter(transcodedFilter).clearGroupByToAggregators();
		ImmutableMap.Builder<ImmutableSet<String>, ImmutableSet<String>> transcodedToOriginalBuilder =
				ImmutableMap.builder();

		Multimaps.asMap(query.getGroupByToAggregators()).forEach((groupBy, aggregators) -> {
			ImmutableSet<String> originalK = ImmutableSet.copyOf(groupBy.getSortedColumns());

			IGroupBy groupByIncludingPostFilterColumns;

			{
				Map<String, IAdhocColumn> columnToDetails = new LinkedHashMap<>();

				columnToDetails.putAll(groupBy.getSortedNameToColumn());

				for (String postFilterColumn : nonPushdownColumns) {
					if (!columnToDetails.containsKey(postFilterColumn)) {
						columnToDetails.put(postFilterColumn, ReferencedColumn.ref(postFilterColumn));
					}
				}

				groupByIncludingPostFilterColumns = GroupByColumns.of(columnToDetails.values());
			}

			IGroupBy transcodedGroupBy = transcodeGroupBy(transcodingContext, groupByIncludingPostFilterColumns);
			// Reverse-alias to user-facing names: records arrive at `transcodeRows` with user-facing column names,
			// while `transcodedGroupBy` carries table-side names; map keys must match what records carry.
			ImmutableSet<String> userFacingTranscodedK = transcodedGroupBy.getSortedColumns().stream().flatMap(c -> {
				Set<String> queried = transcodingContext.queried(c);
				if (queried.isEmpty()) {
					return Stream.of(c);
				} else {
					return queried.stream();
				}
			}).collect(ImmutableSet.toImmutableSet());
			if (!userFacingTranscodedK.equals(originalK)) {
				transcodedToOriginalBuilder.put(userFacingTranscodedK, originalK);
			}

			transcodedQueryBuilder.groupByToAggregators(transcodedGroupBy,
					FilteredAggregatorTranscoder
							.transcode(aggregators, transcodingContext, f -> transcodeFilter(transcodingContext, f)));
		});

		return TranscodedResult.builder()
				.transcodedQuery(transcodedQueryBuilder.build())
				.transcodedToOriginal(transcodedToOriginalBuilder.build())
				.build();
	}

	protected Set<String> getFiltrableCalculatedColumns(TableQueryV4 query) {
		// Calculated columns from ColumnsManager (static definitions)
		Set<String> calculatedColumns = this.calculatedColumns.stream()
				.map(ICalculatedColumn::getName)
				.collect(Collectors.toCollection(TreeSet::new));
		// Calculated columns from Query (dynamic definitions)
		query.getGroupByToAggregators()
				.keySet()
				.stream()
				.flatMap(gb -> gb.getColumns().stream())
				.filter(c -> c instanceof ICalculatedColumn
						&& !(c instanceof FunctionCalculatedColumn f && f.isSkipFiltering()))
				.map(IHasName::getName)
				.forEach(calculatedColumns::add);
		return calculatedColumns;
	}

	/**
	 * 
	 * @param transcodingContext
	 * @param tabularRecordStream
	 * @param postFilter
	 *            a filter to apply over the table rows. Typically used for filter over {@link ICalculatedColumn}. When
	 *            we were not able to predicate pushdown.
	 * @return
	 */
	protected ITabularRecordStream transcodeRows(AliasingContext transcodingContext,
			ITabularRecordStream tabularRecordStream,
			ISliceFilter postFilter,
			TranscodedResult transcodedQuery) {
		return new TranscodingTabularRecordStream(this,
				transcodingContext,
				tabularRecordStream,
				postFilter,
				transcodedQuery);
	}

	protected boolean filterCalculatedColumns(FilterMatcher postFilterer, ITabularGroupByRecord row) {
		return postFilterer.match(row);
	}

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

			@Override
			public boolean isIdentity() {
				return transcodingContext.isIdentity();
			}
		};
	}

	protected ITabularRecord evaluateCalculated(AliasingContext aliasingContext, ITabularRecord row) {
		Map<String, ICalculatedColumn> columns = aliasingContext.getNameToCalculated();

		if (columns.isEmpty()) {
			return row;
		}

		Map<String, Object> computed = new LinkedHashMap<>();

		GroupByColumnsBuilder groupByWithCalculated = GroupByColumns.builder().columns(row.getGroupBy().getColumns());
		columns.forEach((columnName, column) -> {
			// TODO handle recursive formulas (e.g. a formula relying on another formula)
			computed.put(columnName, column.computeCoordinate(row));
			groupByWithCalculated.column(column);
		});

		return TabularRecordOverMaps.builder()
				.slice(groupByWithCalculated.build(), row.asSlice().addColumns(computed))
				.aggregates(row.aggregatesAsMap())
				.build();
	}

	protected IColumnValueTranscoder prepareTypeTranscoder(AliasingContext aliasingContext) {
		Set<String> mayBeTypeTranscoded = aliasingContext.underlyings()
				.stream()
				.filter(customTypeManager::mayTranscode)
				.collect(ImmutableSet.toImmutableSet());

		return new IColumnValueTranscoder() {

			@Override
			public Set<String> mayTranscode() {
				return mayBeTypeTranscoded;
			}

			@Override
			public Set<String> mayTranscode(Set<String> recordColumns) {
				return Sets.intersection(mayBeTypeTranscoded, recordColumns);
			}

			@Override
			public @Nullable Object transcodeValue(String column, @Nullable Object value) {
				return customTypeManager.fromTable(column, value);
			}
		};
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

	protected IGroupBy transcodeGroupBy(AliasingContext aliasingContext, IGroupBy groupBy) {
		NavigableMap<String, IAdhocColumn> nameToColumn = groupBy.getSortedNameToColumn();

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
					} else if (c instanceof ICalculatedColumn calculatedColumn) {
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
								.level(Level.WARN)
								.messageT("BEWARE If %s should be impacted by transcoding", expressionColumn)
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

	@Override
	public Object onMissingColumn(String column) {
		return missingColumnManager.onMissingColumn(column);
	}

	@Override
	public Object onMissingColumn(IHasName cube, String column) {
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
			return ImmutableSet.of();
		}
	}

}
