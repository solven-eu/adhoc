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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

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
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.IRowsStream;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IAdhocTableReverseTranscoder;
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
	public IRowsStream openTableStream(IAdhocTableWrapper table, TableQuery query) {
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
			log.info("[DEBUG] Transcoded query is `{}` given `{}`", transcodedQuery, query);
		}

		{
			List<String> columns = TableQuery.makeSelectedColumns(transcodedQuery);
			if (columns.size() != columns.stream().distinct().count()) {
				// This limitation may be lifted, by having a AdhocRecord which splits clearly the aggregated columns
				// and the groupBy columns
				throw new IllegalArgumentException(
						"Some columns are both used as aggregators and as groupBy: %s".formatted(transcodedQuery));
			}
		}

		IRowsStream rowStream = table.streamSlices(transcodedQuery);

		return transcodeRows(transcodingContext, transcodedQuery, rowStream);
	}

	protected Map<String, ?> transcodeFromDb(IAdhocTableReverseTranscoder transcodingContext,
			Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcodingContext, underlyingMap);
	}

	protected IRowsStream transcodeRows(TranscodingContext transcodingContext,
			TableQuery tableQuery,
			IRowsStream openDbStream) {
		Supplier<Stream<Map<String, ?>>> memoized = Suppliers.memoize(openDbStream::asMap);

		return new IRowsStream() {

			@Override
			public void close() throws Exception {
				// TODO This would open even if it was not closed yet
				memoized.get().close();
			}

			@Override
			public Stream<Map<String, ?>> asMap() {
				return memoized.get()
						// .map(notTranscoded -> {
						// // We transcode only groupBy columns, as an aggregator may have a name matching an underlying
						// column
						// Map<String, ?> transcoded = transcodeFromDb(transcodingContext, notTranscoded);
						//
						// return transcoded;
						//
						// })
						.<Map<String, ?>>map(notTranscoded -> {
							Map<String, Object> aggregatorValues = new LinkedHashMap<>();
							tableQuery.getAggregators().forEach(a -> {
								String aggregatorName = a.getName();
								Object aggregatedValue = notTranscoded.remove(aggregatorName);
								if (aggregatedValue == null) {
									// SQL groupBy returns `a=null` even if there is not a single matching row
									notTranscoded.remove(aggregatorName);
								} else {
									aggregatorValues.put(aggregatorName, aggregatedValue);
								}
							});

							if (aggregatorValues.isEmpty()) {
								// There is not a single non-null aggregate: discard the whole Map (including groupedBy
								// columns)
								// BEWARE When is this relevant? Why not returning only materialized sliced, even if
								// there is
								// not a
								// single column? One poor argument is we (may? always?) add `COUNT(*)` if there is no
								// explicit
								// measure.
								return Map.of();
							} else {
								// In case of manual filters, we may have to hide some some columns, needed by the
								// manual filter, but unexpected by the output stream

								Map<String, ?> transcoded = transcodeFromDb(transcodingContext, notTranscoded);

								// ImmutableMap does not accept null value. How should we handle missing value in
								// groupBy, when returned as null by DB?
								ImmutableMap.Builder<String, Object> transcodedBuilder =
										ImmutableMap.<String, Object>builderWithExpectedSize(
												transcoded.size() + aggregatorValues.size());

								transcoded.forEach((column, value) -> {
									Object nonNullValue;

									if (value == null) {
										// May happen on a failed JOIN: most DB returns null
										// BEWARE If the column is also used as aggregator name, we are setting a
										// default value as aggregator: this may lead to issues
										nonNullValue = getMissingColumnManager().onMissingColumn(column);
									} else {
										nonNullValue = value;
									}

									transcodedBuilder.put(column, nonNullValue);
								});

								Set<String> conflictingKeys =
										Sets.intersection(transcoded.keySet(), aggregatorValues.keySet());
								if (conflictingKeys.isEmpty()) {
									transcodedBuilder.putAll(aggregatorValues);
								} else {
									// This log should be detected before hand, not on each entry
									log.info("[DEBUG] Conflicting keys: {}", conflictingKeys);

									aggregatorValues.entrySet()
											.stream()
											.filter(e -> !conflictingKeys.contains(e.getKey()))
											.forEach(e -> transcodedBuilder.put(e.getKey(), e.getValue()));
								}

								return transcodedBuilder.build();

								// Map<String, Object> merged = new LinkedHashMap<>();
								//
								// merged.putAll(notTranscoded);
								// merged.putAll(aggregatorValues);
								// return merged;
							}
						})
						// Filter-out the groups which does not have a single aggregatedValue
						.filter(m -> !m.isEmpty());

			}

			@Override
			public String toString() {
				return "Transcoding: " + openDbStream;
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
				log.info("BEWARE If {} should be impacted by transcoding", calculatedColumn);
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
