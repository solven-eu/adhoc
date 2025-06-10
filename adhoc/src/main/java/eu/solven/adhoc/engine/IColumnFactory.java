/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.engine;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumn;
import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;

/**
 * Holds the strategy to create {@link IMultitypeColumn} and {@link SliceToValue}.
 *
 * @author Benoit Lacelle
 */
public interface IColumnFactory {

	/**
	 *
	 * @param underlyings
	 * @return a column which will hold result for the given underlyings
	 */
    IMultitypeColumnFastGet<SliceAsMap> makeColumn(List<? extends ISliceToValue> underlyings);

	/**
	 * @param agg
	 * @param underlyings
	 * @return a column which will hold result for the given underlyings, allowing multiple writing (through merge) for the same slice.
	 *
	 */
    IMultitypeMergeableColumn<SliceAsMap> makeColumn(IAggregation agg, List<? extends ISliceToValue> underlyings);

    Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings);
}
