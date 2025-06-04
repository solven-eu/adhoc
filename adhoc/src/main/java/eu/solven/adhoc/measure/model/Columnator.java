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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.measure.transformator.step.ColumnatorQueryStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Columnator} is a {@link IMeasure} which applies its logic only if given columns are expressed. More
 * specifically, it requires given columns to have a simple `EqualsMatcher` (i.e. to be mono-selected).
 * 
 * If the flag `required` is turned to `false`, this turns into a `rejected` behavior: given columns must not be
 * filtered at all.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Jacksonized
@Slf4j
// BEWARE This is a poorly named class. It shall be renamed at some point.
public class Columnator implements ICombinator {
	/**
	 * Different modes describing how columns has to be hidden.
	 * 
	 * @author Benoit Lacelle
	 */
	// https://stackoverflow.com/questions/3069743/coding-conventions-naming-enums
	@SuppressWarnings("PMD.FieldNamingConventions")
	public enum Mode {
		// required: the selected columns are required in the slice, else null
		HideIfMissing,
		// rejected: the select columns must be missing from the slice, else null
		HideIfPresent,
	}

	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	@Singular
	ImmutableSet<String> columns;

	// required=true: the selected columns are required in the slice.
	// else rejected: the select columns must be missing from the slice.
	@Default
	Mode mode = Mode.HideIfMissing;

	@NonNull
	@Singular
	ImmutableList<String> underlyings;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Singular
	ImmutableMap<String, ?> combinationOptions;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return underlyings;
	}

	@Override
	public ITransformatorQueryStep wrapNode(IOperatorsFactory transformationFactory, CubeQueryStep step) {
		return new ColumnatorQueryStep(this, transformationFactory, step);
	}

}
