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
package eu.solven.adhoc.util;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.data.cell.IValueReceiver;

/**
 * Centralizes all implementations doing nothing given input data.
 * 
 * @author Benoit Lacelle
 */
public class AdhocBlackHole implements IValueReceiver, IAdhocEventBus {
	private static final Supplier<AdhocBlackHole> MEMOIZED = Suppliers.memoize(() -> new AdhocBlackHole());

	public static AdhocBlackHole getInstance() {
		return MEMOIZED.get();
	}

	@Override
	public void onLong(long v) {
		// do nothing with the value
	}

	@Override
	public void onDouble(double v) {
		// do nothing with the value
	}

	@Override
	public void onObject(Object v) {
		// do nothing with the value
	}

	@Override
	public void post(Object event) {
		// do nothing with given event
	}
}
