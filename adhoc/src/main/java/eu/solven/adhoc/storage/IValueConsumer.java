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
package eu.solven.adhoc.storage;

/**
 * Able to consume a value which may be a different types, with the ability to handle primitive types without boxing.
 * 
 * This can be seen as a proxy to **read** values.
 * 
 * @author Benoit Lacelle
 * @see IValueProvider
 */
@FunctionalInterface
public interface IValueConsumer {

	/**
	 * If this holds a long, override this optional method to receive the primitive long
	 * 
	 * @param v
	 */
	default void onLong(long v) {
		onObject(v);
	}

	/**
	 * If this holds a double, override this optional method to receive the primitive double
	 * 
	 * @param v
	 */
	default void onDouble(double v) {
		onObject(v);
	}

	/**
	 * If this holds a {@link CharSequence}, override this optional method to receive the {@link CharSequence}
	 * 
	 * @param v
	 */
	default void onCharsequence(CharSequence v) {
		onObject(v);
	}

	void onObject(Object object);
}
