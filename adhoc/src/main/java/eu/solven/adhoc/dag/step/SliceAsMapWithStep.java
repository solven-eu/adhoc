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
package eu.solven.adhoc.dag.step;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

/**
 * A simple {@link IAdhocSlice} based on a {@link Map}
 */
@Builder
@ToString
public class SliceAsMapWithStep implements ISliceWithStep {
	@NonNull
	final SliceAsMap slice;

	@NonNull
	final AdhocQueryStep queryStep;

	@Override
	public @NonNull AdhocQueryStep getQueryStep() {
		return queryStep;
	}

	@Override
	public Set<String> getColumns() {
		return slice.getColumns();
	}

	@Override
	public IAdhocFilter asFilter() {
		// AND the slice with the step as the step may express some filters which are not in the slice
		// e.g. if we filter color=red and groupBy country: slice would express only country=FR
		return AndFilter.and(slice.asFilter(), queryStep.getFilter());
	}

	@Override
	public Optional<Object> optSliced(String column) {
		return slice.optSliced(column);
	}

	@Override
	public SliceAsMap getAdhocSliceAsMap() {
		// BEWARE Make sure we return an existing SliceAsMap, and not creating a new instance
		// We want to minimize the number of different SliceAsMap through a query
		return slice;
	}

	@Override
	public Map<String, ?> optSliced(Set<String> columns) {
		return slice.optSliced(columns);
	}

}
