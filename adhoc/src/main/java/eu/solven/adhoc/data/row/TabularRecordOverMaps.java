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
package eu.solven.adhoc.data.row;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.ITableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.With;

/**
 * A simple {@link ITabularRecord} based on {@link Map}.
 *
 * @author Benoit Lacelle
 */
@Builder
public class TabularRecordOverMaps implements ITabularRecord {
	@NonNull
	@With
	final Map<String, ?> slice;
	// BEWARE: ImmutableMap will forbid null value
	@NonNull
	@Singular
	final ImmutableMap<String, ?> aggregates;

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

		if (AdhocPrimitiveHelpers.isLongLike(aggregate)) {
			return valueReceiver -> valueReceiver.onLong(AdhocPrimitiveHelpers.asLong(aggregate));
		} else if (AdhocPrimitiveHelpers.isDoubleLike(aggregate)) {
			return valueReceiver -> valueReceiver.onDouble(AdhocPrimitiveHelpers.asDouble(aggregate));
		} else {
			return valueReceiver -> valueReceiver.onObject(aggregate);
		}
	}

	@Override
	public Set<String> groupByKeySet() {
		return slice.keySet();
	}

	@Override
	public Object getGroupBy(String columnName) {
		return slice.get(columnName);
	}

	@Override
	public Map<String, ?> asMap() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		asMap.putAll(aggregates);
		asMap.putAll(slice);

		return asMap;
	}

	public static ITabularRecord empty() {
		return TabularRecordOverMaps.builder().aggregates(Map.of()).slice(Map.of()).build();
	}

	@Override
	public boolean isEmpty() {
		return aggregates.isEmpty() && slice.isEmpty();
	}

	@Override
	public Map<String, ?> getGroupBys() {
		return slice;
	}

	@Override
	public ITabularRecord transcode(ITableReverseTranscoder transcodingContext) {
		Map<String, ?> transcodedSlice = AdhocTranscodingHelper.transcodeColumns(transcodingContext, slice);

		return withSlice(transcodedSlice);
	}

	@Override
	public ITabularRecord transcode(IColumnValueTranscoder customValueTranscoder) {
		Map<String, ?> transcodedSlice = AdhocTranscodingHelper.transcodeValues(customValueTranscoder, slice);

		return withSlice(transcodedSlice);
	}

	@Override
	public String toString() {
		return toString(this);
	}

	@SuppressWarnings({ "PMD.InsufficientStringBufferDeclaration", "PMD.ConsecutiveAppendsShouldReuse" })
	public static String toString(ITabularRecord tabularRecord) {
		StringBuilder string = new StringBuilder();

		string.append("slice:{");
		string.append(tabularRecord.groupByKeySet()
				.stream()
				.map(column -> column + "=" + tabularRecord.getGroupBy(column))
				.collect(Collectors.joining(", ")));
		string.append("} aggregates:{");

		string.append(tabularRecord.aggregateKeySet()
				.stream()
				.filter(aggregateName -> null != tabularRecord.getAggregate(aggregateName))
				.map(aggregateName -> aggregateName + "=" + tabularRecord.getAggregate(aggregateName))
				.collect(Collectors.joining(", ")));
		string.append('}');

		return string.toString();
	}
}
