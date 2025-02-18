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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.eventbus.AdhocLogEvent;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.groupby.CalculatedColumn;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.record.IAggregatedRecord;
import eu.solven.adhoc.record.IAggregatedRecordStream;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;
import eu.solven.adhoc.util.IAdhocEventBus;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder(toBuilder = true)
@Slf4j
public class AdhocColumnsManager implements IAdhocColumnsManager {

	@NonNull
	@Default
	final IAdhocEventBus eventBus = IAdhocEventBus.BLACK_HOLE;

	@Default
	@NonNull
	@Getter
	final IAdhocTableTranscoder transcoder = new IdentityImplicitTranscoder();

	@NonNull
	@Default
	final IMissingColumnManager missingColumnManager = new DefaultMissingColumnManager();

	@NonNull
	@Default
	final ICustomTypeManager customTypeManager = new DefaultCustomTypeManager();

	@Override
	public IAggregatedRecordStream openTableStream(IAdhocTableWrapper table, TableQuery query) {
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
		TableQuery transcodedQuery = TableQuery.edit(query)
				.filter(transcodedFilter)
				.groupBy(transcodeGroupBy(transcodingContext, query.getGroupBy()))
				.clearAggregators()
				.aggregators(transcodeAggregators(transcodingContext, query.getAggregators()))
				.build();

		if (query.isDebug()) {
			eventBus.post(AdhocLogEvent.builder()
					.debug(true)
					.message("Transcoded query is `%s` given `%s`".formatted(transcodedQuery, query))
					.source(this)
					.build());
		}

		IAggregatedRecordStream aggregatedRecordsStream = table.streamSlices(transcodedQuery);

		return transcodeRows(transcodingContext, aggregatedRecordsStream);
	}

	protected IAggregatedRecordStream transcodeRows(TranscodingContext transcodingContext,
			IAggregatedRecordStream aggregatedRecordsStream) {
		Supplier<Stream<IAggregatedRecord>> memoized = Suppliers.memoize(aggregatedRecordsStream::asMap);

		return new IAggregatedRecordStream() {

			@Override
			public void close() throws Exception {
				// TODO This would open even if it was not closed yet
				memoized.get().close();
			}

			@Override
			public Stream<IAggregatedRecord> asMap() {
				return memoized.get().map(notTranscoded -> notTranscoded.transcode(transcodingContext));
			}

			@Override
			public String toString() {
				return "Transcoding: " + aggregatedRecordsStream;
			}
		};
	}

	protected TranscodingContext openTranscodingContext() {
		return TranscodingContext.builder().transcoder(getTranscoder()).build();
	}

	protected IAdhocFilter transcodeFilter(TranscodingContext transcodingContext, IAdhocFilter filter) {
		// TODO Transcode types

		if (filter.isMatchAll() || filter.isMatchNone()) {
			return filter;
		} else if (filter instanceof IColumnFilter columnFilter) {
			return ColumnFilter.builder()
					.column(transcodingContext.underlying(columnFilter.getColumn()))
					.valueMatcher(columnFilter.getValueMatcher())
					.build();
		} else if (filter instanceof IAndFilter andFilter) {
			return AndFilter.and(andFilter.getOperands()
					.stream()
					.map(operand -> transcodeFilter(transcodingContext, operand))
					.toList());
		} else if (filter instanceof IOrFilter orFilter) {
			return OrFilter.or(orFilter.getOperands()
					.stream()
					.map(operand -> transcodeFilter(transcodingContext, operand))
					.toList());
		} else if (filter instanceof INotFilter notFilter) {
			return NotFilter.not(transcodeFilter(transcodingContext, notFilter.getNegated()));
		} else {
			throw new UnsupportedOperationException(
					"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	protected IAdhocGroupBy transcodeGroupBy(TranscodingContext transcodingContext, IAdhocGroupBy groupBy) {
		NavigableMap<String, IAdhocColumn> nameToColumn = groupBy.getNameToColumn();

		List<IAdhocColumn> transcoded = nameToColumn.values().stream().<IAdhocColumn>map(c -> {
			if (c instanceof ReferencedColumn referencedColumn) {
				return ReferencedColumn.ref(transcodingContext.underlying(referencedColumn.getColumn()));
			} else if (c instanceof CalculatedColumn calculatedColumn) {
				eventBus.post(AdhocLogEvent.builder()
						.warn(true)
						.message("BEWARE If %s should be impacted by transcoding".formatted(calculatedColumn))
						.source(this)
						.build());
				return calculatedColumn;
			} else {
				throw new UnsupportedOperationException(
						"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(c)));
			}
		}).toList();

		return GroupByColumns.of(transcoded);
	}

	protected Collection<? extends Aggregator> transcodeAggregators(TranscodingContext transcodingContext,
			Set<Aggregator> aggregators) {
		return aggregators.stream()
				.map(a -> Aggregator.edit(a).columnName(transcodingContext.underlying(a.getColumnName())).build())
				.collect(Collectors.toSet());
	}

	@Override
	public Object onMissingColumn(String column) {
		return getMissingColumnManager().onMissingColumn(column);
	}

}
