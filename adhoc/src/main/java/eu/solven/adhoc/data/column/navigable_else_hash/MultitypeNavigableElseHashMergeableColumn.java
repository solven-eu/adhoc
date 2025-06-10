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
package eu.solven.adhoc.data.column.navigable_else_hash;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.column.IColumnScanner;
import eu.solven.adhoc.data.column.IColumnValueConverter;
import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.StreamStrategy;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.data.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.data.column.navigable.MultitypeNavigableMergeableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasure;
import eu.solven.adhoc.measure.transformator.iterator.UnderlyingQueryStepHelpersNavigableElseHash;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * A {@link IMultitypeMergeableColumn} based on {@link MultitypeNavigableElseHashColumn}
 *
 * @param <T>
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class MultitypeNavigableElseHashMergeableColumn<T extends Comparable<T>> extends MultitypeNavigableElseHashColumn<T>
		implements IMultitypeMergeableColumn<T> {

	public static <T extends Comparable<T>> MultitypeNavigableElseHashMergeableColumnBuilder<T, ?, ?> builder(IAggregation aggregation) {
		return new MultitypeNavigableElseHashMergeableColumnBuilderImpl<T>()
				.aggregation(aggregation)
				.navigable(MultitypeNavigableMergeableColumn.<T>builder().aggregation(aggregation).build())
				.hash(MultitypeHashMergeableColumn.<T>builder().aggregation(aggregation).build())
				;
	}


	@NonNull
	@Getter
	IAggregation aggregation;


	@Override
	public IValueReceiver merge(T slice) {
		Optional<IValueReceiver> navigableReceiver = navigable.appendIfOptimal(slice);

        return navigableReceiver.orElseGet(() -> hash.append(slice));
	}
}
