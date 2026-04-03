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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Futures;

/**
 * Marks a data structure that is physically split into N independent partitions.
 *
 * <p>
 * Callers can use {@link PartitioningHelpers#getPartitionIndex(Object, int)} to map an arbitrary key to its owning
 * partition index, then retrieve that partition via {@link #getPartition(int)} to operate on it directly — for example
 * to fill partitions independently across threads before combining them for reading.
 *
 * @param <P>
 *            the type of each individual partition
 * @author Benoit Lacelle
 */
public interface IPartitioned<P> {

	/**
	 * @return the number of partitions; always strictly positive.
	 */
	int getNbPartitions();

	/**
	 * Returns the partition at the given zero-based index.
	 *
	 * @param index
	 *            a value in {@code [0, getNbPartitions())}
	 * @return the partition responsible for that index
	 */
	P getPartition(int index);

	default P getPartition(Object o) {
		return getPartition(PartitioningHelpers.getPartitionIndex(o, getNbPartitions()));
	}

	default <O> List<O> map(Function<P, O> function) {
		return IntStream.range(0, getNbPartitions()).mapToObj(this::getPartition).map(function).toList();
	}

	// Always parallel: assume parallelization given we are partitioned
	default <O> List<O> map(ExecutorService es, Function<P, O> function) {
		Future<List<O>> future = es.<List<O>>submit(() -> {
			return IntStream.range(0, getNbPartitions()).parallel().mapToObj(this::getPartition).map(function).toList();
		});
		return Futures.getUnchecked(future);
	}
}
