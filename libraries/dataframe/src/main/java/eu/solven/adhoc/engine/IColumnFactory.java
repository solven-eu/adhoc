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
package eu.solven.adhoc.engine;

import java.util.List;
import java.util.function.Consumer;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * Holds the strategy to create {@link IMultitypeColumn} and {@link Cuboid}.
 *
 * @author Benoit Lacelle
 */
public interface IColumnFactory {
	/**
	 * To be provided as capacity if we have no clue about its value.
	 */
	int NO_ESTIMATION = 0;

	<T> IMultitypeColumnFastGet<T> makeColumn(ColumnParams<T> params);

	<T> IMultitypeColumnFastGet<T> makeColumn(Consumer<? super ColumnParams.ColumnParamsBuilder<T>> params);

	<T> IMultitypeMergeableColumn<T> makeMergeableColumn(Consumer<? super ColumnParams.ColumnParamsBuilder<T>> params);

	IMultitypeIntColumnFastGet makeIntColumn(Consumer<? super ColumnParams.ColumnParamsBuilder<Integer>> params);

	/**
	 * This is similar to an SQL JOIN over cuboids: we want to align each individual cuboid on a per-slice basis,
	 * mapping to the list of measures.
	 * 
	 * @param step
	 * @param underlyings
	 * @return
	 */
	IConsumingStream<SliceAndMeasures> joinCuboids(CubeQueryStep step, List<? extends ICuboid> underlyings);

	/**
	 * Customize the characteristics of a {@link IMultitypeColumnFastGet}.
	 * 
	 * @param <T>
	 */
	@Builder
	@Value
	class ColumnParams<T> {
		// Is the key an `Integer`? Typically used for AAggregatingColumns
		// @Default
		// boolean isInt = false;
		// Do we insert in random access or not?
		@Default
		Class<T> clazz = null;

		/**
		 * This method should be used when we know the insertion order is random, hence we should not rely on a
		 * Navigable contract.
		 */
		@Default
		boolean isRandomAccess = false;

		/**
		 * 0 is no estimation is available. If strictly positive, the actual capacity may be capped to this
		 */
		@Default
		int initialCapacity = NO_ESTIMATION;

		/**
		 * a column which will hold result for the given underlyings, allowing multiple writing (through merge) for the
		 * same slice.
		 */
		// If null, no merging capabilities
		@Default
		IAggregation agg = null;

		public boolean isInt() {
			return clazz != null && Integer.class.isAssignableFrom(clazz);
		}
	}
}
