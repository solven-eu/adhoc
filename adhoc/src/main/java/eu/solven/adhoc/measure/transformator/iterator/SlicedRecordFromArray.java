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

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.transformator.AdhocDebug;
import lombok.Builder;
import lombok.Singular;

/**
 * A simple {@link ISlicedRecord} based on a {@link List}. It is sub-optimal for performance, but very useful for
 * unit-tests.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class SlicedRecordFromArray implements ISlicedRecord {
	// ImmutableList will not accept `null`. Should we introduce a placeholder for null?
	@Singular
	final List<?> measures;

	@Override
	public boolean isEmpty() {
		return measures.isEmpty();
	}

	@Override
	public int size() {
		return measures.size();
	}

	@Override
	public IValueProvider read(int index) {
		return vc -> vc.onObject(measures.get(index));
	}

	@Override
	public String toString() {
		// Some measure may be an int[]
		return measures.stream().map(AdhocDebug::toString).collect(Collectors.joining(", ", "[", "]"));
	}

}
