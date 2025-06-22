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
package eu.solven.adhoc.measure.transformator;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.measure.combination.ICombination;

/**
 * Enables processing an {@link ICombination} along columns, without having to create {@link IValueReceiver} for each
 * row.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
public interface ICombinationBinding {

	ICombinationBinding NULL = null;

	static ICombinationBinding return0() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 
	 * 
	 * @author Benoit Lacelle
	 */
	@FunctionalInterface
	interface On2 {
		void on3(IValueReceiver valueReceiver, IValueProvider left, IValueProvider right);
	}

	static ICombinationBinding on2(On2 on3) {
		return null;
	}

	/**
	 * 
	 * @return
	 */
	interface IRowState {
		void receive(int index, IValueProvider valueProvider);

		IValueReceiver receive(int index);
	}

}
