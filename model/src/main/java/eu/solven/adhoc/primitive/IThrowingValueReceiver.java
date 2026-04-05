/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.primitive;

/**
 * Able to consume a value which may be a different types, with the ability to handle primitive types without boxing.
 * 
 * Typically, a class would provide a {@link IValueReceiver} to receive data/to be written into.
 * 
 * @author Benoit Lacelle
 * @see IValueProvider
 */
@FunctionalInterface
@SuppressWarnings("PMD.SignatureDeclareThrowsException")
public interface IThrowingValueReceiver extends IValueReceiver {

	@Override
	default void onLong(long v) {
		try {
			onLongMayThrow(v);
		} catch (Exception e) {
			throw new RuntimeException("Issue receiving v=%s".formatted(v), e);
		}
	}

	default void onLongMayThrow(long v) throws Exception {
		onObjectMayThrow(v);
	}

	@Override
	default void onDouble(double v) {
		try {
			onDoubleMayThrow(v);
		} catch (Exception e) {
			throw new RuntimeException("Issue receiving v=%s".formatted(v), e);
		}
	}

	default void onDoubleMayThrow(double v) throws Exception {
		onObjectMayThrow(v);
	}

	@Override
	default void onObject(Object v) {
		try {
			onObjectMayThrow(v);
		} catch (Exception e) {
			throw new RuntimeException("Issue receiving v=%s".formatted(v), e);
		}
	}

	void onObjectMayThrow(Object v) throws Exception;

}
