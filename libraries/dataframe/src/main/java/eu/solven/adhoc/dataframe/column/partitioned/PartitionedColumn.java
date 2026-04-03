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

import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
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
public class PartitionedColumn<T> extends APartitionedColumn<T, IMultitypeColumnFastGet<T>> {

}
