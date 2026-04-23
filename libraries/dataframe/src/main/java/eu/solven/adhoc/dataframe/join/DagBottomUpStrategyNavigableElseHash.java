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
package eu.solven.adhoc.dataframe.join;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGetSorted;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashIntColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableIntColumn;
import eu.solven.adhoc.engine.IColumnFactory.ColumnParams;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.stream.IConsumingStream;

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
 * enables switching automatically to a hash-column, instead of doing many insertions at random index (hence
 * binarySearch) if we stick to a navigable-column.
 * 
 * @author Benoit Lacelle
 */
// TODO Design issue in eu.solven.adhoc.data.column.Cuboid.isSorted() which assume a IMultitypeColumnFastGet is
// sorted or not, while MultitypeNavigableElseHashColumn is partially sorted.
public class DagBottomUpStrategyNavigableElseHash implements IDagBottomUpStrategy {

	// protected IMultitypeMergeableIntColumn makeIntColumnRandomInserts(IAggregation agg, int initialCapacity) {
	// }

	@Override
	public IConsumingStream<SliceAndMeasures> joinCuboids(CubeQueryStep step, List<? extends ICuboid> underlyings) {
		return UnderlyingQueryStepHelpersNavigableElseHash.distinctSlices(step, underlyings);
	}

	@Override
	public <T> IMultitypeColumnFastGet<T> makeColumn(ColumnParams<T> params) {
		int initialCapacity = params.getInitialCapacity();

		if (params.getAgg() != null) {
			return makeMergeable(params);
		} else {
			if (params.isRandomAccess()) {
				return MultitypeHashColumn.<T>builder().capacity(initialCapacity).build();
			} else if (params.isInt()) {
				IMultitypeIntColumnFastGetSorted navigable =
						MultitypeNavigableIntColumn.builder().capacity(initialCapacity).build();
				IMultitypeIntColumnFastGet hash = MultitypeHashIntColumn.builder().capacity(initialCapacity).build();
				return (IMultitypeColumnFastGet<T>) MultitypeNavigableElseHashIntColumn.builder()
						.navigable(navigable)
						.hash(hash)
						.build();
			} else {
				MultitypeNavigableColumn navigable =
						MultitypeNavigableColumn.builder().capacity(initialCapacity).build();
				MultitypeHashColumn<T> hash = MultitypeHashColumn.<T>builder().capacity(initialCapacity).build();
				return MultitypeNavigableElseHashColumn.builder().navigable(navigable).hash(hash).build();
			}
		}
	}

	protected <T> IMultitypeColumnFastGet<T> makeMergeable(ColumnParams<T> params) {
		IAggregation agg = params.getAgg();
		int initialCapacity = params.getInitialCapacity();

		if (params.isRandomAccess()) {
			if (params.isInt()) {
				// Random && int
				return (IMultitypeColumnFastGet<T>) MultitypeHashMergeableIntColumn.builder()
						.aggregation(agg)
						.capacity(initialCapacity)
						.build();
			} else {
				// Random && !int
				return MultitypeHashMergeableColumn.<T>builder().aggregation(agg).capacity(initialCapacity).build();
			}
		} else if (params.isInt()) {
			// !Random && int
			MultitypeNavigableMergeableIntColumn navigable =
					MultitypeNavigableMergeableIntColumn.builder().aggregation(agg).capacity(initialCapacity).build();
			IMultitypeMergeableIntColumn hash =
					MultitypeHashMergeableIntColumn.builder().aggregation(agg).capacity(initialCapacity).build();
			return (IMultitypeColumnFastGet<T>) MultitypeNavigableElseHashMergeableIntColumn.mergeable(agg)
					.navigable(navigable)
					.hash(hash)
					.build();
		} else {
			// !Random && !int
			MultitypeNavigableMergeableColumn navigable =
					MultitypeNavigableMergeableColumn.builder().aggregation(agg).capacity(initialCapacity).build();
			IMultitypeMergeableColumn<Object> hash =
					MultitypeHashMergeableColumn.builder().aggregation(agg).capacity(initialCapacity).build();
			return (IMultitypeMergeableColumn) MultitypeNavigableElseHashMergeableColumn.mergeable(agg)
					.navigable(navigable)
					.hash(hash)
					.build();
		}
	}

	@Override
	public <T> IMultitypeMergeableColumn<T> makeMergeableColumn(ColumnParams<T> params) {
		if (params.getAgg() == null) {
			throw new IllegalArgumentException("IAggregation is required for mergeable");
		}

		return (IMultitypeMergeableColumn<T>) makeColumn(params);
	}

}
