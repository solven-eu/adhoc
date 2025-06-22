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
package eu.solven.adhoc.measure.decomposition;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * An {@link IDecomposition} which duplicated along each possible next values along given columns. It is typically
 * useful to create cumulative {@link IMeasure}.
 * 
 * BEWARE This is not fully functional. Limitations: the output coordinates has to be specified manually through
 * `columnToCoordinates`.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class CumulatingDecomposition extends DuplicatingDecomposition {
	public static final String K_FILTER_EDITOR = "filterEditor";

	@NonNull
	@Getter(AccessLevel.PROTECTED)
	IFilterEditor filterEditor;

	public CumulatingDecomposition(Map<String, ?> options) {
		super(options);

		filterEditor = MapPathGet.getRequiredAs(options, K_FILTER_EDITOR);
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		IAdhocFilter editedFilter = filterEditor.editFilter(step.getFilter());
		return List.of(MeasurelessQuery.edit(step).filter(editedFilter).build());
	}

	@Override
	protected IValueMatcher getValueMatcher(ISliceWithStep slice, String relevantColumn) {
		IAdhocFilter filter = slice.asFilter();
		IValueMatcher current = FilterHelpers.getValueMatcherLax(filter, relevantColumn);
		IValueMatcher previous = FilterHelpers.getValueMatcherLax(filterEditor.editFilter(filter), relevantColumn);
		IValueMatcher nextSlices = NotMatcher.not(previous);
		return OrMatcher.or(current, nextSlices);
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		// No generated columns, as cumulated columns are pre-existing
		return Map.of();
	}
}
