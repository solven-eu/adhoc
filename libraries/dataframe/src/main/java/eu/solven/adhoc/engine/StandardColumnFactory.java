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
import java.util.stream.Stream;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.join.DagBottomUpStrategyNavigableElseHash;
import eu.solven.adhoc.dataframe.join.IDagBottomUpStrategy;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Default implementation for {@link IColumnFactory}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class StandardColumnFactory implements IColumnFactory {
	@NonNull
	@Default
	private final IDagBottomUpStrategy bottomUpStrategy = new DagBottomUpStrategyNavigableElseHash();

	@Override
	public <T> IMultitypeColumnFastGet<T> makeColumn(int initialCapacity) {
		return bottomUpStrategy.makeColumn(initialCapacity);
	}

	@Override
	public <T> IMultitypeColumnFastGet<T> makeColumnRandomInsertions(int initialCapacity) {
		return bottomUpStrategy.makeColumnRandomInserts(initialCapacity);
	}

	@Override
	public <T> IMultitypeMergeableColumn<T> makeColumn(IAggregation agg, int initialCapacity) {
		return bottomUpStrategy.makeColumn(agg, initialCapacity);
	}

	@Override
	public <T> IMultitypeMergeableColumn<T> makeColumnRandomInsertions(IAggregation agg, int initialCapacity) {
		return bottomUpStrategy.makeColumnRandomInserts(agg, initialCapacity);
	}

	@Override
	public Stream<SliceAndMeasures> joinCuboids(CubeQueryStep step, List<? extends ICuboid> underlyings) {
		return bottomUpStrategy.joinCuboids(step, underlyings);
	}

}
