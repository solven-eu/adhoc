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
package eu.solven.adhoc.dataframe.column.partitioned;

import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.IColumnScanner;
import eu.solven.adhoc.cuboid.IColumnValueConverter;
import eu.solven.adhoc.cuboid.ICompactable;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * A read-only {@link IMultitypeColumnFastGet} assembled from N independently-built partition columns. Keys are routed
 * to their owning partition via {@link PartitioningHelpers#getPartitionIndex(Object, int)}, so each key lives in
 * exactly one partition.
 *
 * <p>
 * This is the read-only counterpart of {@link PartitionedMergeableColumn}: it is typically created after all
 * per-partition writes are complete (e.g. after parallel aggregation) and is then used for sequential scanning or point
 * lookups.
 *
 * @param <T>
 *            the key type (typically {@link eu.solven.adhoc.cuboid.slice.ISlice})
 * @author Benoit Lacelle
 */
@SuperBuilder
public abstract class APartitionedColumn<T, D extends IMultitypeColumnFastGet<T>>
		implements IMultitypeColumnFastGet<T>, IPartitioned<D>, ICompactable {

	/** One independent column per partition; each key maps to exactly one partition. */
	@NonNull
	@Singular
	private final ImmutableList<D> partitions;

	@Override
	public int getNbPartitions() {
		return partitions.size();
	}

	@Override
	public D getPartition(int index) {
		return partitions.get(index);
	}

	/**
	 * Returns the partition that owns {@code key}, delegating index computation to
	 * {@link PartitioningHelpers#getPartitionIndex(Object, int)}.
	 */
	protected D partition(T key) {
		return getPartition(key);
	}

	@Override
	public IValueReceiver append(T key) {
		return partition(key).append(key);
	}

	@Override
	public IValueProvider onValue(T key) {
		return partition(key).onValue(key);
	}

	@Override
	public void scan(IColumnScanner<T> rowScanner) {
		partitions.forEach(p -> p.scan(rowScanner));
	}

	@Override
	public <U> Stream<U> stream(IColumnValueConverter<T, U> converter) {
		return partitions.stream().flatMap(p -> p.stream(converter));
	}

	@Override
	public Stream<SliceAndMeasure<T>> stream() {
		return partitions.stream().flatMap(IMultitypeColumnFastGet::stream);
	}

	@Override
	public Stream<T> keyStream() {
		return partitions.stream().flatMap(IMultitypeColumnFastGet::keyStream);
	}

	@Override
	public long size() {
		return partitions.stream().mapToLong(IMultitypeColumnFastGet::size).sum();
	}

	@Override
	public boolean isEmpty() {
		return partitions.stream().allMatch(IMultitypeColumnFastGet::isEmpty);
	}

	/**
	 * Purges {@link eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier} instances from every partition and
	 * collects the resolved entries into a single flat {@link MultitypeHashColumn}.
	 */
	@Override
	public IMultitypeColumnFastGet<T> purgeAggregationCarriers() {
		List<IMultitypeColumnFastGet<T>> purged =
				map(AdhocUnsafe.adhocCpuPool, IMultitypeColumnFastGet::purgeAggregationCarriers);

		return PartitionedColumn.<T>builder().partitions(purged).build();
	}

	@Override
	public void compact() {
		partitions.forEach(p -> {
			if (p instanceof ICompactable compactable) {
				compactable.compact();
			}
		});
	}
}
