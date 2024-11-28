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
package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Unfiltrator} is an {@link IMeasure} which is removing filtered columns from current {@link AdhocQueryStep}.
 * It is typically useful to make ratios with a parent slice.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Slf4j
public class Unfiltrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	@Singular
	Set<String> tags;

	@NonNull
	String underlying;

	@NonNull
	@Singular
	Set<String> unfiltereds;

	// By default, the unfiltered columns are removed from filters. Switch this to true if you wish to unfiltered all
	// but these columns.
	@Default
	boolean inverse = false;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new UnfiltratorQueryStep(this, transformationFactory, step);
	}

	public static class UnfiltratorBuilder {
		/**
		 * Use this if you want only given columns to remain filtered.
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

}
