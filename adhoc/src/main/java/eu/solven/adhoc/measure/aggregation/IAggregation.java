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
package eu.solven.adhoc.measure.aggregation;

import java.util.List;

import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.storage.IValueProvider;
import eu.solven.adhoc.storage.IValueReceiver;

/**
 * An {@link IAggregation} can turn a {@link List} of values (typically from {@link Combinator}) into a new value.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregation {
	Object aggregate(Object left, Object right);

	// BEWARE It is important not to take a IValueConsumer as argument, it may typically lead to a synchronous
	// `.clearKey` while preparing the write, which may lead to a valueProvider not to get access to some data.
	// By returning an IValueProvider, we delay the initialization of the receiver
	default IValueProvider aggregate(IValueProvider l, IValueProvider r) {
		return new IValueProvider() {

			@Override
			public void acceptConsumer(IValueReceiver valueReceiver) {
				l.acceptConsumer(new IValueReceiver() {

					@Override
					public void onLong(long vL) {
						r.acceptConsumer(new IValueReceiver() {

							@Override
							public void onLong(long vR) {
								if (this instanceof ILongAggregation longAggregation) {
									valueReceiver.onLong(longAggregation.aggregateLongs(vL, vR));
								} else {
									valueReceiver.onObject(aggregate(vL, vR));
								}
							}

							@Override
							public void onObject(Object vR) {
								valueReceiver.onObject(aggregate(vL, vR));
							}
						});
					}

					@Override
					public void onDouble(double vL) {
						r.acceptConsumer(new IValueReceiver() {

							@Override
							public void onDouble(double vR) {
								if (this instanceof IDoubleAggregation doubleAggregation) {
									valueReceiver.onDouble(doubleAggregation.aggregateDoubles(vL, vR));
								} else {
									valueReceiver.onObject(aggregate(vL, vR));
								}
							}

							@Override
							public void onObject(Object vR) {
								valueReceiver.onObject(aggregate(vL, vR));
							}
						});
					}

					@Override
					public void onObject(Object vL) {
						r.acceptConsumer(new IValueReceiver() {

							@Override
							public void onObject(Object vR) {
								valueReceiver.onObject(aggregate(vL, vR));
							}
						});
					}
				});
			}
		};
	}

}
