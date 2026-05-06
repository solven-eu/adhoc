/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.query.table.TableQueryV4;
import lombok.Builder;
import lombok.Value;

/**
 * Result of {@link ColumnsManager#transcodeQuery}: the transcoded {@link TableQueryV4} plus a per-grouping-set map
 * (user-facing transcoded keyset → original keyset) consumed by {@link #project} to align records with what
 * {@code TabularRecordStreamReducer.columnsToMarker} expects.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class TranscodedResult {
	TableQueryV4 transcodedQuery;
	ImmutableMap<ImmutableSet<String>, ImmutableSet<String>> transcodedToOriginal;

	/**
	 * @return {@code true} when at least one grouping set was reshaped — i.e. {@link #project} is needed downstream.
	 */
	protected boolean needsProjection() {
		return !transcodedToOriginal.isEmpty();
	}

	/**
	 * Restricts {@code record} to its original cube-engine groupBy keyset. Lookup key = current keyset minus
	 * {@code calculatedColumnNames}; missing entry means the grouping set was not reshaped and {@code retainAll} is a
	 * no-op.
	 */
	public ITabularRecord project(ITabularRecord record, Set<String> calculatedColumnNames) {
		if (!needsProjection()) {
			return record;
		}

		Set<String> currentKeyset = record.asSlice().columnsKeySet();

		ImmutableSet<String> postCalcKeyset;
		if (calculatedColumnNames.isEmpty()) {
			postCalcKeyset = ImmutableSet.copyOf(currentKeyset);
		} else {
			postCalcKeyset = currentKeyset.stream()
					.filter(c -> !calculatedColumnNames.contains(c))
					.collect(ImmutableSet.toImmutableSet());
		}

		Set<String> originalK = transcodedToOriginal.getOrDefault(postCalcKeyset, postCalcKeyset);
		return record.retainAll(ImmutableSortedSet.copyOf(originalK));
	}
}
