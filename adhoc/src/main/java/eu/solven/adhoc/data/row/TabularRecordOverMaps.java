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

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import lombok.Builder;
import lombok.ToString;

@Builder
@ToString
public class TabularRecordOverMaps implements ITabularRecord {
	final Map<String, ?> aggregates;
	final Map<String, ?> groupBys;

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
			return valueConsumer -> valueConsumer.onLong(AdhocPrimitiveHelpers.asLong(aggregate));
		} else if (AdhocPrimitiveHelpers.isDoubleLike(aggregate)) {
			return valueConsumer -> valueConsumer.onDouble(AdhocPrimitiveHelpers.asDouble(aggregate));
		} else if (aggregate instanceof CharSequence charsequence) {
			return valueConsumer -> valueConsumer.onCharsequence(charsequence);
		} else {
			return valueConsumer -> valueConsumer.onObject(aggregate);
		}
	}

	@Override
	public Set<String> groupByKeySet() {
		return groupBys.keySet();
	}

	@Override
	public Object getGroupBy(String columnName) {
		return groupBys.get(columnName);
	}

	@Override
	public Map<String, ?> asMap() {
		Map<String, Object> asMap = new LinkedHashMap<>();

		asMap.putAll(aggregates);
		asMap.putAll(groupBys);

		return asMap;
	}

	public static ITabularRecord empty() {
		return TabularRecordOverMaps.builder().aggregates(Map.of()).groupBys(Map.of()).build();
	}

	@Override
	public boolean isEmpty() {
		return aggregates.isEmpty() && groupBys.isEmpty();
	}

	@Override
	public Map<String, ?> getGroupBys() {
		return groupBys;
	}

	@Override
	public ITabularRecord transcode(IAdhocTableReverseTranscoder transcodingContext) {
		Map<String, ?> transcodedGroupBys = AdhocTranscodingHelper.transcodeColumns(transcodingContext, groupBys);

		return TabularRecordOverMaps.builder().aggregates(aggregates).groupBys(transcodedGroupBys).build();
	}

	@Override
	public ITabularRecord transcode(ICustomTypeManager customTypeManager) {
		Map<String, ?> transcodedGroupBys =
				AdhocTranscodingHelper.transcodeValues(customTypeManager::fromTable, groupBys);

		return TabularRecordOverMaps.builder().aggregates(aggregates).groupBys(transcodedGroupBys).build();
	}

}
