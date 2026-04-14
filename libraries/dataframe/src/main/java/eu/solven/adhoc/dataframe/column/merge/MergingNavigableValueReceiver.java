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
package eu.solven.adhoc.dataframe.column.merge;

import java.util.function.Consumer;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.ILongAggregation;
import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * 
 * @author Benoit Lacelle
 */
public class MergingNavigableValueReceiver implements IValueReceiver {
	IAggregation aggregation;
	IValueReceiver receiver;
	Consumer<IValueReceiver> consumer;

	public MergingNavigableValueReceiver(IAggregation agg, IValueReceiver receiver, Consumer<IValueReceiver> consumer) {
		this.aggregation = agg;
		this.receiver = receiver;
		this.consumer = consumer;
	}

	@Override
	public void onLong(long input) {
		consumer.accept(new IValueReceiver() {

			@Override
			public void onLong(long existingAggregate) {
				if (aggregation instanceof ILongAggregation longAggregation) {
					long newAggregate = longAggregation.aggregateLongs(existingAggregate, input);
					receiver.onLong(newAggregate);
				} else {
					Object newAggregate = aggregation.aggregate(existingAggregate, input);
					receiver.onObject(newAggregate);
				}
			}

			@Override
			public void onObject(Object existingAggregate) {
				Object newAggregate = aggregation.aggregate(existingAggregate, input);
				receiver.onObject(newAggregate);
			}
		});
	}

	@Override
	public void onObject(Object input) {
		consumer.accept(new IValueReceiver() {

			@Override
			public void onLong(long existingAggregate) {
				Object newAggregate = aggregation.aggregate(existingAggregate, input);
				receiver.onObject(newAggregate);
			}

			@Override
			public void onObject(Object existingAggregate) {
				Object newAggregate = aggregation.aggregate(existingAggregate, input);
				receiver.onObject(newAggregate);
			}
		});
	}
}