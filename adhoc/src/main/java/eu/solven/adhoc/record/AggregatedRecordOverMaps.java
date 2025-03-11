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
package eu.solven.adhoc.record;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.storage.IValueReceiver;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;
import lombok.Builder;
import lombok.ToString;

@Builder
@ToString
public class AggregatedRecordOverMaps implements IAggregatedRecord {
	final Map<String, ?> aggregates;
	final Map<String, ?> groupBys;

	@Override
	public Set<String> aggregateKeySet() {
		return aggregates.keySet();
	}

	@Override
	public Object getAggregate(String aggregateName) {
		return aggregates.get(aggregateName);
	}

	@Override
	public void onAggregate(String aggregateName, IValueReceiver valueConsumer) {
		Object aggregate = getAggregate(aggregateName);

		if (SumAggregation.isLongLike(aggregate)) {
			valueConsumer.onLong(SumAggregation.asLong(aggregate));
		} else if (SumAggregation.isDoubleLike(aggregate)) {
			valueConsumer.onDouble(SumAggregation.asDouble(aggregate));
		} else if (aggregate instanceof CharSequence charsequence) {
			valueConsumer.onCharsequence(charsequence);
		} else {
			valueConsumer.onObject(aggregate);
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

	public static IAggregatedRecord empty() {
		return AggregatedRecordOverMaps.builder().aggregates(Map.of()).groupBys(Map.of()).build();
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
	public IAggregatedRecord transcode(IAdhocTableReverseTranscoder transcodingContext) {
		Map<String, ?> transcodedGroupBys = AdhocTranscodingHelper.transcodeColumns(transcodingContext, groupBys);

		return AggregatedRecordOverMaps.builder().aggregates(aggregates).groupBys(transcodedGroupBys).build();
	}

	@Override
	public IAggregatedRecord transcode(ICustomTypeManager customTypeManager) {
		Map<String, ?> transcodedGroupBys =
				AdhocTranscodingHelper.transcodeValues(customTypeManager::fromTable, groupBys);

		return AggregatedRecordOverMaps.builder().aggregates(aggregates).groupBys(transcodedGroupBys).build();
	}

}
