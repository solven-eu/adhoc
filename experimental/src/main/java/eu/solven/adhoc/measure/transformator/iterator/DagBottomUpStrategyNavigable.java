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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;

/**
 * Relies on {@link MultitypeNavigableColumn} by default, and {@link UnderlyingQueryStepHelpersNavigable} to merge
 * underlyings.
 * 
 * Known issue: some {@link ITransformatorQueryStep} will not write slices in proper order (e.g. Dispatchor): so they
 * rely on a {@link MultitypeHashColumn}. Hence, dependents measures may receive navigable and hash columns. Given
 * merging strategy will fallback into a distinctNotSorted {@link Iterator}, which is not efficient given the default
 * {@link MultitypeNavigableColumn} (of the dependent).
 * 
 * @author Benoit Lacelle
 */
public class DagBottomUpStrategyNavigable implements IDagBottomUpStrategy {

	@Override
	public IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeNavigableColumn.<SliceAsMap>builder().build();
	}

	@Override
	public Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpersNavigable.distinctSlices(step, underlyings);
	}

}
