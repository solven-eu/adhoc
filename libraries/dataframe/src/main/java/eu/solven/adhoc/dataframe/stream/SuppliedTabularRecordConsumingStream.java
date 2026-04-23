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
package eu.solven.adhoc.dataframe.stream;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.stream.IConsumingStream;

/**
 * A {@link ITabularRecordStream} memorizing an underlying `Stream<Map<String, ?>>`
 * 
 * @author Benoit Lacelle
 */
public class SuppliedTabularRecordConsumingStream implements ITabularRecordStream {
	final Object source;
	final boolean isDistinct;
	final Supplier<IConsumingStream<ITabularRecord>> streamSupplier;

	public SuppliedTabularRecordConsumingStream(Object source,
			boolean isDistinct,
			Supplier<IConsumingStream<ITabularRecord>> streamSupplier) {
		this.source = source;
		this.isDistinct = isDistinct;
		// Memoize the stream to make sure it is open only once
		this.streamSupplier = Suppliers.memoize(streamSupplier::get);
	}

	@Override
	public IConsumingStream<ITabularRecord> records() {
		return streamSupplier.get();
	}

	@Override
	public void close() {
		streamSupplier.get().close();
	}

	@Override
	public String toString() {
		return "source=%s".formatted(source);
	}

	@Override
	public boolean isDistinctSlices() {
		return isDistinct;
	}
}
