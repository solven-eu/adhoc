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

import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * An {@link IMultitypeMergeableColumn} that distributes keys across N independent partitions using hash routing. Each
 * {@link #merge} or {@link #append} call is forwarded to exactly one partition determined by
 * {@code Math.floorMod(key.hashCode(), nbPartitions)}.
 *
 * <p>
 * The main benefit over a single flat column is reduced per-partition map size: with K distinct keys spread over N
 * partitions, each internal hash map holds on average K/N entries, which improves cache locality and reduces re-hashing
 * overhead for large key sets.
 *
 * <p>
 * Each partition is independently a {@link MultitypeHashMergeableColumn}: hash-based routing removes any
 * insertion-order guarantee, so the navigable optimisation would be irrelevant.
 *
 * <p>
 * Instances can also be assembled from pre-built partition columns via the {@link #builder()}, enabling callers to fill
 * each partition independently (e.g. one thread per partition) and then present them as a unified column for reading.
 *
 * @param <T>
 *            the key type (typically {@link eu.solven.adhoc.cuboid.slice.ISlice})
 * @author Benoit Lacelle
 */
@SuperBuilder
public class PartitionedMergeableColumn<T> extends APartitionedColumn<T, IMultitypeMergeableColumn<T>>
		implements IMultitypeMergeableColumn<T> {

	@NonNull
	@Getter
	private final IAggregation aggregation;

	@Override
	public IValueReceiver merge(T key) {
		return partition(key).merge(key);
	}

}
