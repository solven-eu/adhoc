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
package eu.solven.adhoc.dataframe.row;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.cuboid.tabular.ITabularGroupByRecord;
import eu.solven.adhoc.map.AdhocMapHelpers;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.ITableReverseAliaser;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.With;

/**
 * A simple {@link ITabularRecord} based on {@link Map}.
 *
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
@AllArgsConstructor
@EqualsAndHashCode
public class TabularRecordOverMaps implements ITabularRecord {
	@NonNull
	@With
	final ITabularGroupByRecord groupBy;
	// BEWARE: ImmutableMap will forbid null value
	@NonNull
	@Singular
	final ImmutableMap<String, ?> aggregates;

	@Override
	public IGroupBy getGroupBy() {
		return groupBy.getGroupBy();
	}

	@Override
	public Set<String> aggregateKeySet() {
		return aggregates.keySet();
	}

	@Override
	public Map<String, ?> aggregatesAsMap() {
		return aggregates;
	}

	@Override
	public Object getAggregate(String aggregateName) {
		return aggregates.get(aggregateName);
	}

	@Override
	public IValueProvider onAggregate(String aggregateName) {
		Object aggregate = getAggregate(aggregateName);

		// TODO Should we normalize at construction?
		return AdhocPrimitiveHelpers.normalizeValueAsProvider(aggregate);
	}

	@Override
	public Set<String> columnsKeySet() {
		return groupBy.columnsKeySet();
	}

	@Override
	public Object getGroupBy(String columnName) {
		return groupBy.getGroupBy(columnName);
	}

	@Override
	public Optional<Object> optGroupBy(String column) {
		return groupBy.optGroupBy(column);
	}

	@Override
	public Map<String, ?> asMap() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		asMap.putAll(aggregates);
		asMap.putAll(groupBy.asSlice().asAdhocMap());

		return asMap;
	}

	public static ITabularRecord empty() {
		return TabularRecordOverMaps.builder()
				.aggregates(Map.of())
				.slice(IGroupBy.GRAND_TOTAL, SliceHelpers.grandTotal())
				.build();
	}

	@Override
	public ISlice asSlice() {
		return groupBy.asSlice();
	}

	protected ITabularRecord withSlice(ISliceFactory factory, Map<String, ?> slice) {
		return withGroupBy(groupByRecord(groupBy.getGroupBy(), AdhocMapHelpers.fromMap(factory, slice).asSlice()));
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	@Override
	public ITabularRecord transcode(ITableReverseAliaser transcodingContext) {
		IAdhocMap inputMap = groupBy.asSlice().asAdhocMap();
		Map<String, ?> outputMap = AdhocTranscodingHelper.transcodeColumns(transcodingContext, inputMap);

		if (outputMap == inputMap) {
			return this;
		}

		return withSlice(groupBy.asSlice().getFactory(), outputMap);
	}

	@Override
	public ITabularRecord transcode(IColumnValueTranscoder customValueTranscoder) {
		Map<String, ?> transcoded =
				AdhocTranscodingHelper.transcodeValues(customValueTranscoder, groupBy.asSlice().asAdhocMap());

		return withSlice(this.groupBy.asSlice().getFactory(), transcoded);
	}

	@Override
	public String toString() {
		return toString(this);
	}

	@Override
	public void forEachGroupBy(BiConsumer<? super String, ? super Object> action) {
		groupBy.forEachGroupBy(action);
	}

	@SuppressWarnings({ "PMD.InsufficientStringBufferDeclaration", "PMD.ConsecutiveAppendsShouldReuse" })
	public static String toString(ITabularRecord tabularRecord) {
		StringBuilder sb = new StringBuilder();

		sb.append("slice:{");
		sb.append(tabularRecord.columnsKeySet()
				.stream()
				.map(column -> column + "=" + tabularRecord.getGroupBy(column))
				.collect(Collectors.joining(", ")));
		sb.append("} aggregates:{");

		sb.append(tabularRecord.aggregateKeySet()
				.stream()
				.filter(aggregateName -> null != tabularRecord.getAggregate(aggregateName))
				.map(aggregateName -> aggregateName + "=" + tabularRecord.getAggregate(aggregateName))
				.collect(Collectors.joining(", ")));
		sb.append('}');

		return sb.toString();
	}

	protected static TabularGroupByRecordOverMap groupByRecord(IGroupBy groupBy, ISlice slice) {
		return TabularGroupByRecordOverMap.builder().groupBy(groupBy).slice(slice).build();
	}

	/**
	 * Lombok @Builder
	 *
	 * @author Benoit Lacelle
	 */
	public static class TabularRecordOverMapsBuilder {
		// public TabularRecordOverMapsBuilder slice(IAdhocSlice slice) {
		// return slice(GroupByColumns.named(slice.columnsKeySet()), slice);
		// }

		public TabularRecordOverMapsBuilder slice(IGroupBy groupBy, ISlice slice) {
			return groupBy(groupByRecord(groupBy, slice));
		}

	}

	@Override
	public ITabularRecord retainAll(NavigableSet<String> columns) {
		return withGroupBy(groupBy.retainAll(columns));
	}
}
