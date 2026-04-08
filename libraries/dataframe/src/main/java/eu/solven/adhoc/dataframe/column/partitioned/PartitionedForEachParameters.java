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

import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import eu.solven.adhoc.dataframe.stream.IConsumingStream;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Parameters for {@link PartitioningHelpers#forEachPartitioned(PartitionedForEachParameters)}.
 *
 * @param <T>
 *            the element type
 * @author Benoit Lacelle
 */
@Value
@Builder
public class PartitionedForEachParameters<T> {

	/**
	 * The source stream to consume.
	 */
	@NonNull
	IConsumingStream<T> stream;

	/**
	 * Number of independent consumer threads to create.
	 */
	int nbPartitions;

	/**
	 * Maps each element to a partition index in {@code [0, nbPartitions)}.
	 */
	@NonNull
	ToIntFunction<T> partitioner;

	/**
	 * The action to perform on each element, called from the owning partition's thread.
	 */
	@NonNull
	Consumer<T> consumer;

	/**
	 * The executor used to run partition consumer threads.
	 */
	@NonNull
	Executor executor;

	/**
	 * Bounded capacity per partition queue (in number of batches). Applies backpressure so the producer cannot get far
	 * ahead of slow consumers.
	 */
	@Builder.Default
	int queueCapacity = AdhocUnsafe.getQueueCapacity();

	/**
	 * Number of elements accumulated per partition before dispatching a batch to the queue. Larger values reduce queue
	 * put/take overhead at the cost of slightly higher latency for the first elements.
	 */
	@Builder.Default
	int batchSize = 256;
}
