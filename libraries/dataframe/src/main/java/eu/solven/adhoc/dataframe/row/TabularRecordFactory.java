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

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.map.keyset.SequencedSetUnsafe;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

/**
 * Enable creating {@link TabularRecordBuilder} given an {@link AggregatedRecordFields}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@AllArgsConstructor
public class TabularRecordFactory implements ITabularRecordFactory {

	@NonNull
	AggregatedRecordFields fields;
	@NonNull
	ISliceFactory sliceFactory;
	@Singular
	@Getter
	ImmutableList<String> optionalColumns;

	// A non-ambiguous mapping from columnName to column
	// It is not compatible with all TableQueryV3, as different groupBy may have different columns referring the same
	// columnName
	// Some earlier step shall split the tableQuery to prevent such ambiguities
	@NonNull
	IGroupBy globalGroupBy;

	@Override
	public List<String> getAggregates() {
		return fields.getAggregates();
	}

	@Override
	public ImmutableSet<String> getColumns() {
		return fields.getAllColumns();
	}

	@Override
	public TabularRecordBuilder makeTabularRecordBuilder(Set<String> absentColumns) {
		ImmutableMap.Builder<String, Object> aggregates = ImmutableMap.builderWithExpectedSize(getAggregates().size());

		Iterable<? extends String> presentColumns = getColumns(absentColumns);
		IMapBuilderPreKeys sliceBuilder = sliceFactory.newMapBuilder(presentColumns);

		SequencedSetLikeList columns = SequencedSetUnsafe.internKeyset(ImmutableSet.copyOf(presentColumns));

		return new TabularRecordBuilder(globalGroupBy.retainAll(columns.sortedSet()), aggregates, sliceBuilder);
	}

	protected Iterable<? extends String> getColumns(Set<String> absentColumns) {
		if (absentColumns.isEmpty()) {
			return getColumns();
		} else {
			if (!getColumns().containsAll(absentColumns)) {
				throw new IllegalArgumentException(
						"Some absent are unknown. absent=%s known=%s".formatted(absentColumns, getColumns()));
			}
			return Sets.difference(getColumns(), absentColumns);
		}
	}

}
