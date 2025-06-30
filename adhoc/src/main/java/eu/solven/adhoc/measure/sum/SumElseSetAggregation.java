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
package eu.solven.adhoc.measure.sum;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import lombok.extern.slf4j.Slf4j;

/**
 * A `SUM` {@link IAggregation}, managing not Number as error objects. As long as we encounter summable input, we do
 * sum. Else we aggregate `errors` into a Set.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/sum-function-dax
@Slf4j
public class SumElseSetAggregation extends SumAggregation {

	public static final String KEY = "SUM_ELSE_SET";

	@Override
	protected Collection<?> aggregateCollections(Collection<?> left, Collection<?> right) {
		int limitSize = checkCollectionsSizes(left, right);

		return Stream.concat(left.stream(), right.stream()).limit(limitSize).collect(ImmutableSet.toImmutableSet());
	}

	@Override
	protected Object wrapNotANumber(Object r) {
		if (r instanceof Collection<?> asCollection) {
			return asCollection.stream().filter(Objects::nonNull).collect(Collectors.toSet());
		} else {
			// Wrap into a Set, so this aggregate function return either a long/double, or a Set of errors
			return Set.of(r);
		}
	}

}
