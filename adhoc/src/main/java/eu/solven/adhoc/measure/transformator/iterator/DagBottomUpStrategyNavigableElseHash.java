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
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Shiftor;

/**
 * This `v1` improves `v0` by relying on {@link MultitypeNavigableElseHashColumn}, which can handle a large number of
 * slices through a navigable structure, and keep the leftovers (expected to be received at the end) into a hash
 * structure.
 * 
 * `distinctSlices` will returns a {@link Stream} by merging in a first pass the slices from navigable structure, and
 * hashed-slices in a second pass.
 * 
 * It is expected to give better results on measure having a {@link Dispatchor} or {@link Shiftor} underlying: these
 * relies on a hash-column, but a measure depending on them may also receive underlyings with a navigable-column: we
 * should give priority to navigable ordering, and process hashes as left-overs. It would also give better result as it
 * enables switching automatically to a hash-column, instead of doing many insertions at random index if we stick to a
 * navigable-column.
 * 
 * @author Benoit Lacelle
 */
// TODO Design issue in eu.solven.adhoc.data.column.SliceToValue.isSorted() which assume a IMultitypeColumnFastGet is
// sorted or not, while MultitypeNavigableElseHashColumn is partially sorted.
public class DagBottomUpStrategyNavigableElseHash implements IDagBottomUpStrategy {

	@Override
	public IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeNavigableElseHashColumn.<SliceAsMap>builder().build();
	}

	@Override
	public Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpersNavigableElseHash.distinctSlices(step, underlyings);
	}

}
