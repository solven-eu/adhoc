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
package eu.solven.adhoc.dataframe.column.navigable_else_hash;

import java.util.Optional;

import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableIntColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link IMultitypeMergeableColumn} based on {@link MultitypeNavigableElseHashColumn}
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class MultitypeNavigableElseHashMergeableIntColumn extends MultitypeNavigableElseHashIntColumn
		implements IMultitypeMergeableIntColumn {
	@NonNull
	@Getter
	IAggregation aggregation;

	public static MultitypeNavigableElseHashMergeableIntColumnBuilder<?, ?> mergeable(IAggregation aggregation) {
		return new MultitypeNavigableElseHashMergeableIntColumnBuilderImpl().aggregation(aggregation)
				.navigable(MultitypeNavigableMergeableIntColumn.builder().aggregation(aggregation).build())
				.hash(MultitypeHashMergeableIntColumn.builder().aggregation(aggregation).build());
	}

	@Override
	public IValueReceiver merge(int slice) {
		Optional<IValueReceiver> navigableReceiver = navigable.appendIfOptimal(slice, false);

		return navigableReceiver.orElseGet(() -> hash.append(slice));
	}
}
