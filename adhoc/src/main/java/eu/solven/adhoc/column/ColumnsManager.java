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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.MoreFilterHelpers;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;
import eu.solven.adhoc.table.transcoder.value.DefaultCustomTypeManager;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
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

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
@Slf4j
public class ColumnsManager implements IColumnsManager {

	@NonNull
	@Default
	final IAdhocEventBus eventBus = IAdhocEventBus.BLACK_HOLE;

	@Default
	@NonNull
	@Getter
	final ITableTranscoder transcoder = new IdentityImplicitTranscoder();

	@NonNull
	@Default
	final IMissingColumnManager missingColumnManager = new DefaultMissingColumnManager();

	@NonNull
	@Default
	final ICustomTypeManager customTypeManager = new DefaultCustomTypeManager();

	@NonNull
	@Singular
	final ImmutableSet<CalculatedColumn> calculatedColumns;

	@Override
	public String transcodeToTable(String cubeColumn) {
		return transcoder.underlyingNonNull(cubeColumn);
	}

	@Override
	public ITabularRecordStream openTableStream(QueryPod queryPod, TableQueryV2 query) {
		TranscodingContext transcodingContext = openTranscodingContext();

		IAdhocFilter transcodedFilter;
		{
			IAdhocFilter notTranscodedFilter = query.getFilter();
			transcodedFilter = transcodeFilter(transcodingContext, notTranscodedFilter);

			transcodingContext.underlyings().forEach(underlying -> {
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

		ITableWrapper table = queryPod.getTable();
		ITabularRecordStream aggregatedRecordsStream = table.streamSlices(queryPod, transcodedQuery);

		return transcodeRows(transcodingContext, aggregatedRecordsStream);
	}

	protected ITabularRecordStream transcodeRows(TranscodingContext transcodingContext,
			ITabularRecordStream aggregatedRecordsStream) {
		return new ITabularRecordStream() {

			@Override
			public void close() {
				aggregatedRecordsStream.close();
			}

			@Override
			public Stream<ITabularRecord> records() {
				return aggregatedRecordsStream.records()
						.map(notTranscoded -> notTranscoded.transcode(transcodingContext))
						.map(row -> transcodeTypes(row))
						.map(row -> transcodeCalculated(transcodingContext, row));
			}

			@Override
			public String toString() {
				return "Transcoding: " + aggregatedRecordsStream;
			}
		};
	}

	protected ITabularRecord transcodeCalculated(TranscodingContext transcodingContext, ITabularRecord row) {
		Map<String, CalculatedColumn> columns = transcodingContext.getNameToCalculated();

		if (columns.isEmpty()) {
			return row;
		}

		Map<String, Object> enrichedGroupBy = new LinkedHashMap<>(row.getGroupBys());

		columns.forEach((columnName, column) -> {
			// TODO handle recursive formulas (e.g. a formula relying on another formula)
			enrichedGroupBy.put(columnName, column.computeCoordinate(row));
		});

		return TabularRecordOverMaps.builder().aggregates(row.aggregatesAsMap()).slice(enrichedGroupBy).build();
	}

	protected ITabularRecord transcodeTypes(ITabularRecord row) {
		return row.transcode(customTypeManager);
	}

	protected TranscodingContext openTranscodingContext() {
		return TranscodingContext.builder().transcoder(getTranscoder()).build();
	}

	protected IAdhocFilter transcodeFilter(ITableTranscoder tableTranscoder, IAdhocFilter filter) {
		return MoreFilterHelpers.transcodeFilter(customTypeManager, tableTranscoder, filter);
	}

	// protected IValueMatcher transcodeType(String column, IValueMatcher valueMatcher) {
	// return MoreFilterHelpers.transcodeType(customTypeManager, column, valueMatcher);
	// }

	protected IAdhocGroupBy transcodeGroupBy(TranscodingContext transcodingContext, IAdhocGroupBy groupBy) {
		NavigableMap<String, IAdhocColumn> nameToColumn = groupBy.getNameToColumn();

		List<IAdhocColumn> transcoded = nameToColumn.values().stream().<IAdhocColumn>flatMap(c -> {
			if (c instanceof ReferencedColumn referencedColumn) {
				return Stream.of(ReferencedColumn.ref(transcodingContext.underlying(referencedColumn.getName())));
			} else if (c instanceof ExpressionColumn expressionColumn) {
				// BEWARE To handle transcoding, one would need to parse the SQL, to replace columns references
				eventBus.post(AdhocLogEvent.builder()
						.warn(true)
						.message("BEWARE If %s should be impacted by transcoding".formatted(expressionColumn))
						.source(this)
						.build());
				return Stream.of(expressionColumn);
			} else if (c instanceof CalculatedColumn calculatedColumn) {
				transcodingContext.addCalculatedColumn(calculatedColumn);

				eventBus.post(AdhocLogEvent.builder()
						.warn(true)
						.message("BEWARE If %s should be impacted by transcoding".formatted(calculatedColumn))
						.source(this)
						.build());
				return getUnderlyingColumns(calculatedColumn).stream();
			} else {
				throw new UnsupportedOperationException(
						"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(c)));
			}
		}).toList();

		return GroupByColumns.of(transcoded);
	}

	private static class RecordingRecord implements ITabularRecord {
		@Getter
		final Set<String> usedColumn = new HashSet<>();

		@Override
		public Set<String> aggregateKeySet() {
			throw new UnsupportedOperationException("Not .keySet() else it would register all columns as underlying");
		}

		@Override
		public Object getAggregate(String aggregateName) {
			throw new NotYetImplementedException("Calculated Column over aggregates");
		}

		@Override
		public IValueProvider onAggregate(String aggregateName) {
			throw new NotYetImplementedException("Calculated Column over aggregates");
		}

		@Override
		public Map<String, ?> aggregatesAsMap() {
			throw new UnsupportedOperationException("Not .keySet() else it would register all columns as underlying");
		}

		@Override
		public Set<String> groupByKeySet() {
			throw new UnsupportedOperationException("Not .keySet() else it would register all columns as underlying");
		}

		@Override
		public Object getGroupBy(String columnName) {
			usedColumn.add(columnName);
			return null;
		}

		@Override
		public Map<String, ?> asMap() {
			throw new UnsupportedOperationException("Not .keySet() else it would register all columns as underlying");
		}

		@Override
		public boolean isEmpty() {
			// Indicates this is not empty to preventing short-cutting reading any field
			return false;
		}

		@Override
		public Map<String, ?> getGroupBys() {
			throw new UnsupportedOperationException("Not .keySet() else it would register all columns as underlying");
		}

		@Override
		public ITabularRecord transcode(IAdhocTableReverseTranscoder transcodingContext) {
			throw new UnsupportedOperationException("Recording does not implement this");
		}

		@Override
		public ITabularRecord transcode(ICustomTypeManager customTypeManager) {
			throw new UnsupportedOperationException("Recording does not implement this");
		}

	}

	private Collection<ReferencedColumn> getUnderlyingColumns(CalculatedColumn calculatedColumn) {
		RecordingRecord recording = new RecordingRecord();

		calculatedColumn.getRecordToCoordinate().apply(recording);
		return recording.getUsedColumn().stream().map(c -> ReferencedColumn.ref(c)).toList();
	}

	protected Collection<? extends FilteredAggregator> transcodeAggregators(TranscodingContext transcodingContext,
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

		return ImmutableMap.copyOf(columnToType);
	}

}
