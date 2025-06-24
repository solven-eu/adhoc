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
import eu.solven.adhoc.data.cell.MultitypeCell;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.util.AdhocBlackHole;

/**
 * Enables processing an {@link ICombination} along columns, without having to create {@link IValueReceiver} for each
 * row.
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "Not-Ready")
public interface ICombinationBinding {

	static ICombinationBinding atIndex(int bindedIndex) {

		/**
		 * A ICombinationBinding which transmit the IValueProvider at given index.
		 */
		return new ICombinationBinding() {
			MultitypeCell receiver = MultitypeCell.builder().aggregation(new CoalesceAggregation()).build();

			IValueProvider consumer = new IValueProvider() {

				@Override
				public void acceptReceiver(IValueReceiver valueReceiver) {
					receiver.acceptReceiver(valueReceiver);
				}
			};

			@Override
			public void reset(ISliceWithStep slice) {
				receiver.clear();
			}

			@Override
			public IValueReceiver readUnderlying(int underlyingIndex) {
				if (underlyingIndex == bindedIndex) {
					return receiver;
				} else {
					return AdhocBlackHole.getInstance();
				}
			}

			@Override
			public IValueProvider reduce() {
				return consumer;
			}

		};
	}

	/**
	 * Reset the state for given slice.
	 * 
	 * @param slice
	 */
	void reset(ISliceWithStep slice);

	/**
	 * 
	 * @param underlyingIndex
	 * @return a {@link IValueReceiver} for given underlyingIndex.
	 */
	IValueReceiver readUnderlying(int underlyingIndex);

	IValueProvider reduce();

	/**
	 * 
	 * @return a {@link ICombinationBinding} which always returns null.
	 */
	static ICombinationBinding empty() {
		return new ICombinationBinding() {

			@Override
			public void reset(ISliceWithStep slice) {
				// nothing to do
			}

			@Override
			public IValueReceiver readUnderlying(int underlyingIndex) {
				return AdhocBlackHole.getInstance();
			}

			@Override
			public IValueProvider reduce() {
				return IValueProvider.NULL;
			}

		};
	}

}
