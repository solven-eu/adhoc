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
package eu.solven.adhoc.data.cell;

import java.util.concurrent.atomic.AtomicReference;

/**
 * While {@link IValueReceiver} can be interpreted as a way to transmit data, {@link IValueProvider} is a way to
 * transmit data in the opposite direction.
 * <p>
 * {@link IValueProvider} can be seen as a way to send data/to be read from.
 * 
 * @author Benoit Lacelle
 * @see IValueReceiver
 */
public interface IValueProvider {

	/**
	 * Constant {@link IValueProvider} always providing `null`.
	 */
	IValueProvider NULL = vc -> vc.onObject(null);

	/**
	 * Will transmit this value into the provider {@link IValueReceiver}
	 * 
	 * @param valueReceiver
	 */
	void acceptReceiver(IValueReceiver valueReceiver);

	/**
	 * Helper method to convert a {@link IValueProvider} into a plain {@link Object}
	 * 
	 * @param valueProvider
	 * @return
	 */
	static Object getValue(IValueProvider valueProvider) {
		AtomicReference<Object> refV = new AtomicReference<>();

		valueProvider.acceptReceiver(refV::set);

		return refV.get();
	}

	/**
	 * Helper method to convert a plain {@link Object} into a {@link IValueProvider}
	 * 
	 * @param o
	 * @return
	 */
	static IValueProvider setValue(Object o) {
		return vr -> vr.onObject(o);
	}
}
