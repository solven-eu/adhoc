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

import eu.solven.adhoc.dataframe.column.IAppendOnlyMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.join.UnderlyingQueryStepHelpersNavigableElseHash;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.AccessLevel;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link IMultitypeColumnFastGet} enabling to fallback on hash-based {@link IMultitypeColumnFastGet}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 * @see UnderlyingQueryStepHelpersNavigableElseHash
 */
@SuperBuilder
@Slf4j
@ToString
public class MultitypeNavigableElseHashColumn<T extends Comparable<T>> extends AMultitypeNavigableElseHashColumn<T>
		implements IAppendOnlyMultitypeColumn<T> {
	@Default
	@NonNull
	@Getter(AccessLevel.PROTECTED)
	final IMultitypeColumnFastGetSorted<T> navigable = MultitypeNavigableColumn.<T>builder().build();

	@Default
	@NonNull
	@Getter(AccessLevel.PROTECTED)
	final IMultitypeColumnFastGet<T> hash = MultitypeHashColumn.<T>builder().build();

	@Override
	public IMultitypeColumnFastGet<T> purgeAggregationCarriers() {
		return MultitypeNavigableElseHashColumn.<T>builder()
				.navigable(getNavigable().purgeAggregationCarriers())
				.hash(getHash().purgeAggregationCarriers())
				.build();
	}

	@Override
	public IValueReceiver appendNew(T slice) {
		Optional<IValueReceiver> navigableReceiver = getNavigable().appendIfOptimal(slice, true);

		return navigableReceiver.orElseGet(() -> getHash().append(slice));
	}

}