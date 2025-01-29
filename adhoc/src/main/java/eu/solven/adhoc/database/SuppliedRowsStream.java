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
package eu.solven.adhoc.database;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;

/**
 * A {@link IRowsStream} memorizing an underlying `Stream<Map<String, ?>>`
 */
public class SuppliedRowsStream implements IRowsStream {
	final Supplier<Stream<Map<String, ?>>> mapStreamSupplier;

	public SuppliedRowsStream(Supplier<Stream<Map<String, ?>>> mapStreamSupplier) {
		// Memoize the stream to make sure it is open only once
		this.mapStreamSupplier = Suppliers.memoize(mapStreamSupplier::get);
	}

	@Override
	public Stream<Map<String, ?>> asMap() {
		return mapStreamSupplier.get();
	}

	@Override
	public void close() throws Exception {
		mapStreamSupplier.get().close();
	}
}
