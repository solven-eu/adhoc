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
package eu.solven.adhoc.stream.partitioned;

import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.dataframe.column.partitioned.IPartitioned;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.Builder;
import lombok.Singular;

/**
 * Wraps a {@link List} of partitions of {@link IConsumingStream}.
 * 
 * @param <T>
 * @author Benoit Lacelle
 */
@Builder
public class PartitionedConsumingStream<T> implements IConsumingStream<T>, IPartitioned<IConsumingStream<T>> {
	@Singular
	ImmutableList<IConsumingStream<T>> partitions;

	@Override
	public int getNbPartitions() {
		return partitions.size();
	}

	@Override
	public IConsumingStream<T> getPartition(int index) {
		return partitions.get(index);
	}

	@Override
	public void forEach(Consumer<T> consumer) {
		partitions.forEach(p -> p.forEach(consumer));
	}

	@Override
	public void close() {
		partitions.forEach(IConsumingStream::close);
	}

}
