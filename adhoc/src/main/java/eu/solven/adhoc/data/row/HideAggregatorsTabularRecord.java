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

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.table.transcoder.ITableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import lombok.Builder;
import lombok.NonNull;
import lombok.With;

/**
 * Decorate a {@link ITabularRecord}, and shows only given Set of aggregates.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class HideAggregatorsTabularRecord implements ITabularRecord {
	@With
	@NonNull
	final ITabularRecord decorated;
	// Not Singular not to build an ImmutableSet of each instance
	// @Singular
	@NonNull
	final ImmutableSet<String> keptAggregates;

	@Override
	public Set<String> aggregateKeySet() {
		return keptAggregates;
	}

	@Override
	public IValueProvider onAggregate(String aggregateName) {
		if (keptAggregates.contains(aggregateName)) {
			return decorated.onAggregate(aggregateName);
		} else {
			return IValueProvider.NULL;
		}
	}

	@Override
	public Map<String, ?> aggregatesAsMap() {
		Map<String, Object> copy = LinkedHashMap.newLinkedHashMap(keptAggregates.size());

		keptAggregates.forEach(keptAggregate -> {
			decorated.onAggregate(keptAggregate).acceptReceiver(o -> {
				// May be null when hiding an unknown aggregateName
				if (o != null) {
					copy.put(keptAggregate, o);
				}
			});
		});

		return copy;
	}

	@Override
	public Set<String> groupByKeySet() {
		return decorated.groupByKeySet();
	}

	@Override
	public Object getGroupBy(String columnName) {
		return decorated.getGroupBy(columnName);
	}

	@Override
	public Map<String, ?> asMap() {
		Map<String, Object> copy = new LinkedHashMap<>(aggregatesAsMap());

		groupByKeySet().forEach(columnName -> {
			copy.put(columnName, getGroupBy(columnName));
		});

		return copy;
	}

	@Override
	public IAdhocSlice getGroupBys() {
		return decorated.getGroupBys();
	}

	@Override
	public ITabularRecord transcode(ITableReverseTranscoder transcodingContext) {
		return withDecorated(decorated.transcode(transcodingContext));
	}

	@Override
	public ITabularRecord transcode(IColumnValueTranscoder customValueTranscoder) {
		return withDecorated(decorated.transcode(customValueTranscoder));
	}

	@Override
	public String toString() {
		return TabularRecordOverMaps.toString(this);
	}
}
