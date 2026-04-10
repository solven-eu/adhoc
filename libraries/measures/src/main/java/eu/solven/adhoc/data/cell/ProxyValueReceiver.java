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

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.AdhocBlackHole;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.RequiredArgsConstructor;

/**
 * Helps intercepting the value sent to an {@link IValueReceiver}
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
public class ProxyValueReceiver implements IValueReceiver {
	private static final IAggregation COALESCE = new CoalesceAggregation();

	@Default
	final IValueReceiver proxied = AdhocBlackHole.getInstance();
	final IMultitypeCell recorder = MultitypeCell.builder().aggregation(COALESCE).build();

	@Override
	public void onLong(long v) {
		proxied.onLong(v);
		recorder.merge().onLong(v);
	}

	@Override
	public void onDouble(double v) {
		proxied.onDouble(v);
		recorder.merge().onDouble(v);
	}

	@Override
	public void onObject(Object v) {
		proxied.onObject(v);
		recorder.merge().onObject(v);
	}

	public IValueProvider asValueProvider() {
		return recorder.reduce();
	}

	@Override
	public String toString() {
		return "recorded: " + IValueProvider.getValue(recorder.reduce());
	}
}