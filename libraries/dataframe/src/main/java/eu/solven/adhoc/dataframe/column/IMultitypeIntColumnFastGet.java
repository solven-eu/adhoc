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
package eu.solven.adhoc.dataframe.column;

import eu.solven.adhoc.cuboid.StreamStrategy;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;

/**
 * {@link IMultitypeColumnFastGet} specialized for {@link Integer} key.
 *
 * @author Benoit Lacelle
 */
public interface IMultitypeIntColumnFastGet extends IMultitypeColumnFastGet<Integer> {

	IValueProvider onValue(int key);

	@Override
	default IValueProvider onValue(Integer key) {
		return onValue(key.intValue());
	}

	default IValueProvider onValue(int key, StreamStrategy strategy) {
		return switch (strategy) {
		case StreamStrategy.ALL:
			// As we assume there is no sorted leg, the complement is all
		case StreamStrategy.SORTED_SUB_COMPLEMENT:
			yield onValue(key);
		case StreamStrategy.SORTED_SUB:
			// Assume there is no sorted leg
			yield IValueProvider.NULL;
		};
	}

	@Override
	default IValueProvider onValue(Integer key, StreamStrategy strategy) {
		return onValue(key.intValue());
	}

	IValueReceiver append(int key);

	@Override
	default IValueReceiver append(Integer key) {
		return append(key.intValue());
	}

	/**
	 * Covariant narrowing of {@link IMultitypeColumnFastGet#purgeAggregationCarriers}: the purge of an int-specialized
	 * column is always itself int-specialized. Callers (notably {@code UndictionarizedColumn.purgeAggregationCarriers})
	 * can therefore re-wrap the result without a secondary instance-check.
	 */
	@Override
	IMultitypeIntColumnFastGet purgeAggregationCarriers();
}
