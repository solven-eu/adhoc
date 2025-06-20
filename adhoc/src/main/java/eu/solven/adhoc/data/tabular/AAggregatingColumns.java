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
package eu.solven.adhoc.data.tabular;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.tabular.primitives.Object2IntBiConsumer;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.query.table.IAliasedAggregator;
import it.unimi.dsi.fastutil.objects.AbstractObject2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Common behavior, given specialization would typically change their behavior depending on if
 * {@link ITabularRecordStream} is distinctSlices or not.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public abstract class AAggregatingColumns<T extends Comparable<T>, K> implements IMultitypeMergeableGrid<T> {

	@NonNull
	@Default
	IOperatorFactory operatorFactory = new StandardOperatorFactory();

	protected abstract int dictionarize(T key);

	protected abstract IMultitypeColumn<K> getColumn(IAliasedAggregator aggregator);

	@Override
	public long size(IAliasedAggregator aggregator) {
		long size = 0L;

		IMultitypeColumn<?> preColumn = getColumn(aggregator);
		if (preColumn != null) {
			size += preColumn.size();
		}

		return size;
	}

	// visible for benchmarks
	@SuppressWarnings("PMD.LooseCoupling")
	@Deprecated(since = "Not used anymore")
	public static <T extends Comparable<T>> ObjectArrayList<Object2IntMap.Entry<T>> doSort(
			Consumer<Object2IntBiConsumer<T>> sliceToIndex,
			int size) {
		log.debug("> sorting {}", size);

		// Do not rely on a TreeMap, else the sorting is done one element at a time
		// ObjectArrayList enables calling `Arrays.parallelSort`
		// `.wrap` else will rely on a `Object[]`, which will later fail on `Arrays.parallelSort`
		ObjectArrayList<Object2IntMap.Entry<T>> sortedEntries = ObjectArrayList.wrap(new Object2IntMap.Entry[size], 0);

		sliceToIndex.accept(
				(slice, rowIndex) -> sortedEntries.add(new AbstractObject2IntMap.BasicEntry<>(slice, rowIndex)));

		Arrays.parallelSort(sortedEntries.elements(), Map.Entry.comparingByKey());

		log.debug("< sorting {}", size);

		return sortedEntries;
	}

}
