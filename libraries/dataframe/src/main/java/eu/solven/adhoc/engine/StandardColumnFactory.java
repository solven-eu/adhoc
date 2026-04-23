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
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeIntColumnFastGet;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.join.DagBottomUpStrategyNavigableElseHash;
import eu.solven.adhoc.dataframe.join.IDagBottomUpStrategy;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.IColumnFactory.ColumnParams.ColumnParamsBuilder;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.stream.IConsumingStream;
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
	public <T> IMultitypeColumnFastGet<T> makeColumn(ColumnParams<T> params) {
		return bottomUpStrategy.makeColumn(params);
	}

	@Override
	public <T> IMultitypeColumnFastGet<T> makeColumn(Consumer<? super ColumnParamsBuilder<T>> params) {
		return bottomUpStrategy.makeColumn(buildParameters(params));
	}

	@Override
	public IMultitypeIntColumnFastGet makeIntColumn(Consumer<? super ColumnParamsBuilder<Integer>> params) {
		ColumnParams<Integer> parameters = buildParameters(parametersBuilder -> {
			// Make Integer by default
			parametersBuilder.clazz(Integer.class);
			// Apply the User customizations
			params.accept(parametersBuilder);
		});

		if (!parameters.isInt()) {
			throw new IllegalArgumentException("intColumn but type=%s".formatted(parameters.getClazz()));
		}

		return (IMultitypeIntColumnFastGet) bottomUpStrategy.makeColumn(parameters);
	}

	@Override
	public <T> IMultitypeMergeableColumn<T> makeMergeableColumn(
			Consumer<? super ColumnParams.ColumnParamsBuilder<T>> params) {
		ColumnParams<T> parameters = buildParameters(params);

		if (parameters.getAgg() == null) {
			throw new IllegalArgumentException("mergeable but aggregation=null");
		}

		return (IMultitypeMergeableColumn<T>) bottomUpStrategy.makeColumn(parameters);
	}

	protected <T> ColumnParams<T> buildParameters(Consumer<? super ColumnParams.ColumnParamsBuilder<T>> parameters) {
		ColumnParams.ColumnParamsBuilder<T> paramsBuilder = ColumnParams.builder();
		parameters.accept(paramsBuilder);

		return paramsBuilder.build();
	}

	@Override
	public IConsumingStream<SliceAndMeasures> joinCuboids(CubeQueryStep step, List<? extends ICuboid> underlyings) {
		return bottomUpStrategy.joinCuboids(step, underlyings);
	}

}
