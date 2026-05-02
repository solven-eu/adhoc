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
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Filtrator} is a specialisation of {@link Shiftor} where the {@link IFilterEditor} always ANDs a fixed,
 * hardcoded {@link ISliceFilter} onto the query filter. Because this pattern is extremely common (e.g. "always restrict
 * to {@code country=FR}"), {@link Filtrator} provides a simpler builder that does not require implementing a full
 * {@link IFilterEditor}.
 *
 * <p>
 * The effective filter passed to the underlying measure is:
 *
 * <pre>
 * effective = queryStep.filter AND filtrator.filter
 * </pre>
 *
 * <p>
 * The result is always written back to the original (un-filtered) slice coordinates.
 *
 * @see Shiftor the generalisation that supports arbitrary filter transformations via {@link IFilterEditor}
 * @see Unfiltrator the counterpart that widens (removes) filter constraints
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Jacksonized
@Slf4j
public class Filtrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	String underlying;

	@NonNull
	ISliceFilter filter;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return ImmutableList.of(underlying);
	}

}
