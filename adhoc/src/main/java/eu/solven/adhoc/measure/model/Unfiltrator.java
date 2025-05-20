/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.model;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.column_generator.IColumnGenerator;
import eu.solven.adhoc.measure.transformator.column_generator.IMayHaveColumnGenerator;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.step.UnfiltratorQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Unfiltrator} is an {@link IMeasure} which is removing filtered columns from current {@link CubeQueryStep}.
 * By removing a filter, we request underlying measures with a wider slice. It is typically useful to make ratios with a
 * parent slice.
 * 
 * If `inverse=true`, the filter is kept only for selected columns, which all others are turned into `matchAll`.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Jacksonized
@Slf4j
public class Unfiltrator implements IMeasure, IHasUnderlyingMeasures, IMayHaveColumnGenerator {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	String underlying;

	@NonNull
	@Singular
	Set<String> unfiltereds;

	// By default, the selected columns are turned to `matchAll`.
	// If true, only selected columns are kept; others are turned into `matchAll`.
	@Default
	boolean inverse = false;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public ITransformatorQueryStep wrapNode(IOperatorsFactory transformationFactory, CubeQueryStep step) {
		return new UnfiltratorQueryStep(this, step);
	}

	public static class UnfiltratorBuilder {
		/**
		 * Use this if you want only given columns to be filtered, while others are turned into `matchAll`.
		 *
		 * @param column
		 * @param moreColumns
		 * @return current builder.
		 */
		public UnfiltratorBuilder filterOnly(String column, String... moreColumns) {
			this.unfiltereds(Lists.asList(column, moreColumns));

			this.inverse(true);

			return this;
		}
	}

	@Override
	public Optional<IColumnGenerator> optColumnGenerator(IOperatorsFactory operatorsFactory) {
		// TODO Auto-generated method stub
		return Optional.empty();
	}

}
