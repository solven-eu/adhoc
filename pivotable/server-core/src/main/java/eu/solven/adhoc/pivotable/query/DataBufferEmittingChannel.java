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
package eu.solven.adhoc.pivotable.query;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;

import reactor.core.publisher.FluxSink;

/**
 * A {@link WritableByteChannel} that forwards each {@link ByteBuffer} write as a {@link DataBuffer} emitted to a
 * {@link FluxSink}. This lets the Arrow IPC writer stream its output directly into a reactive {@code Flux<DataBuffer>}
 * without buffering all bytes in memory first.
 *
 * <p>
 * Each call to {@link #write(ByteBuffer)} allocates exactly one {@link DataBuffer} and calls
 * {@link FluxSink#next(Object)}, so the granularity of emission matches the granularity of Arrow's internal writes
 * (typically one per schema message, one metadata header + one data buffer per column).
 *
 * @author Benoit Lacelle
 */
public class DataBufferEmittingChannel implements WritableByteChannel {

	protected final FluxSink<DataBuffer> sink;
	protected final DataBufferFactory factory;

	private boolean open = true;

	public DataBufferEmittingChannel(FluxSink<DataBuffer> sink, DataBufferFactory factory) {
		this.sink = sink;
		this.factory = factory;
	}

	@Override
	public int write(ByteBuffer src) {
		int remaining = src.remaining();
		byte[] bytes = new byte[remaining];
		src.get(bytes);
		sink.next(factory.wrap(bytes));
		return remaining;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() {
		open = false;
	}
}
