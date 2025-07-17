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
package eu.solven.adhoc.data.column;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

public interface IValueProviderTestHelpers {
	// Pick a seemingly random value. Long.MAX_VALUE may be referred for some edge-cases
	long LONG_NOT_SUPPORTED = Long.MAX_VALUE / 7;
	double DOUBLE_NOT_SUPPORTED = Double.MAX_VALUE / 7;

	static long getLong(IValueProvider valueProvider) {
		AtomicLong longRef = new AtomicLong(LONG_NOT_SUPPORTED);

		valueProvider.acceptReceiver(new IValueReceiver() {

			@Override
			public void onLong(long v) {
				if (v == LONG_NOT_SUPPORTED) {
					throw new IllegalArgumentException("getLong does not handle Long.MAX_VALUE");
				}

				longRef.set(v);
			}

			@Override
			public void onObject(Object v) {
				throw new IllegalArgumentException("getLong requires onLong and not onObject");
			}
		});

		return longRef.get();
	}

	static double getDouble(IValueProvider valueProvider) {
		AtomicDouble doubleRef = new AtomicDouble(DOUBLE_NOT_SUPPORTED);

		valueProvider.acceptReceiver(new IValueReceiver() {

			@Override
			public void onDouble(double v) {
				if (v == DOUBLE_NOT_SUPPORTED) {
					throw new IllegalArgumentException("getDouble does not handle Double.MAX_VALUE");
				}

				doubleRef.set(v);
			}

			@Override
			public void onObject(Object v) {
				throw new IllegalArgumentException("getDouble requires onLong and not onObject");
			}
		});

		return doubleRef.get();
	}
}
