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
package eu.solven.adhoc.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Centralize most `identity` behavior in Adhoc.
 * 
 * @author Benoit Lacelle
 */
public class AdhocIdentity implements IDecomposition, IFilterEditor {
	public static final String KEY = "identity";

	private static final Supplier<AdhocIdentity> MEMOIZED = Suppliers.memoize(() -> new AdhocIdentity());

	public static AdhocIdentity getInstance() {
		return MEMOIZED.get();
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		return Collections.singletonList(IDecompositionEntry.of(Map.of(), value));
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		return Collections.singletonList(step);
	}

	@Override
	public IAdhocFilter editFilter(IAdhocFilter input) {
		return input;
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return CoordinatesSample.empty();
	}

}
