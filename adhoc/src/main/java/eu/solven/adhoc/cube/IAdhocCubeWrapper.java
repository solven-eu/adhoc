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
package eu.solven.adhoc.cube;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.primitives.Ints;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.util.IHasColumns;
import eu.solven.adhoc.util.IHasName;

/**
 * Wrap the cube interface in Adhoc. It is similar to a table over which only aggregate queries are available.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAdhocCubeWrapper extends IHasColumns, IHasName, IHasMeasures {
	default ITabularView execute(IAdhocQuery query) {
		return execute(query, Set.of());
	}

	/**
	 * 
	 * @param query
	 * @param options
	 *            see {@link StandardQueryOptions}
	 * @return
	 */
	ITabularView execute(IAdhocQuery query, Set<? extends IQueryOption> options);

	default CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		ITabularView view = this.execute(AdhocQuery.builder().groupByAlso(column).build());

		int saturatedViewSize = Ints.saturatedCast(view.size());

		int returnedCoordinates = saturatedViewSize;
		if (limit > 0) {
			returnedCoordinates = Math.min(saturatedViewSize, limit);
		}

		int finalReturnedCoordinates = returnedCoordinates;

		List<Object> distinctCoordinates = new ArrayList<>(returnedCoordinates);
		long estimatedCardinality = view.slices().map(slice -> slice.getRawSliced(column)).peek(coordinate -> {
			if (distinctCoordinates.size() < finalReturnedCoordinates) {
				distinctCoordinates.add(coordinate);
			}
		}).count();

		return CoordinatesSample.builder()
				.coordinates(distinctCoordinates)
				.estimatedCardinality(estimatedCardinality)
				.build();
	}

}
