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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.step.UnfiltratorQueryStep;
import eu.solven.adhoc.query.filter.IAdhocFilter;
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
public class Unfiltrator implements IMeasure, IHasUnderlyingMeasures {
	/**
	 * Different mode to modify the {@link IAdhocFilter}.
	 * 
	 * @author Benoit Lacelle
	 */
	// https://stackoverflow.com/questions/3069743/coding-conventions-naming-enums
	@SuppressWarnings("PMD.FieldNamingConventions")
	public enum Mode {
		// if a column is listed, its filters are neutralized into matchAll
		Suppress,
		// if a column is not listed, its filters are neutralized into matchAll
		Retain,
	}

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
	ImmutableSet<String> columns;

	// By default, the selected columns are turned to `matchAll`.
	// If true, only selected columns are kept; others are turned into `matchAll`.
	@Default
	Mode mode = Mode.Suppress;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public ITransformatorQueryStep wrapNode(AdhocFactories factories, CubeQueryStep step) {
		return new UnfiltratorQueryStep(this, step);
	}

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class UnfiltratorBuilder {
		/**
		 * Use this if you want only given columns to be filtered, while others are turned into `matchAll`.
		 *
		 * @param column
		 * @param moreColumns
		 * @return current builder.
		 */
		public UnfiltratorBuilder unfilterOthersThan(String column, String... moreColumns) {
			this.clearColumns().columns(Lists.asList(column, moreColumns));

			this.mode(Mode.Retain);

			return this;
		}
	}

}
