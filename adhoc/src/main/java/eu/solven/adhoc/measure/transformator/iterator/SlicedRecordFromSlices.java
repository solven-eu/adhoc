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
package eu.solven.adhoc.measure.transformator.iterator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.data.row.ISlicedRecord;
import lombok.Builder;

/**
 * A {@link ISlicedRecord} based on a {@link List} of {@link SliceAndMeasure}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class SlicedRecordFromSlices implements ISlicedRecord {
	final List<IValueProvider> valueProviders;

	@Override
	public boolean isEmpty() {
		return valueProviders.isEmpty();
	}

	@Override
	public int size() {
		return valueProviders.size();
	}

	@Override
	public void read(int index, IValueReceiver valueConsumer) {
		valueProviders.get(index).acceptConsumer(valueConsumer);
	}

	@Override
	public String toString() {
		return IntStream.range(0, size()).<String>mapToObj(index -> {
			Object v = IValueProvider.getValue(vc -> read(index, vc));

			return String.valueOf(v);
		}).collect(Collectors.joining(", ", "[", "]"));
	}

}
