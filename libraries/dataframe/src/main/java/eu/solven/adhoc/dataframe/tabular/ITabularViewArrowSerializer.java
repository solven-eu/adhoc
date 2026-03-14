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
package eu.solven.adhoc.dataframe.tabular;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Converts an {@link IReadableTabularView} into an Apache Arrow IPC stream (application/vnd.apache.arrow.stream) and
 * writes it to a {@link WritableByteChannel}.
 *
 * <p>
 * Implementations are discovered at runtime via {@link java.util.ServiceLoader}. The {@code adhoc-experimental} module
 * provides a default implementation backed by the Apache Arrow Java library.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface ITabularViewArrowSerializer {

	/**
	 * Serializes the given view as an Arrow IPC stream and writes all bytes to {@code channel}.
	 *
	 * @param view
	 *            the tabular view to serialize; must be fully materialized before calling this method
	 * @param channel
	 *            the destination channel; the caller is responsible for closing it
	 * @throws IOException
	 *             if writing to the channel fails
	 */
	void serialize(IReadableTabularView view, WritableByteChannel channel) throws IOException;
}
