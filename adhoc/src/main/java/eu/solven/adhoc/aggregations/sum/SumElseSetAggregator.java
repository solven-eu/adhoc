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
package eu.solven.adhoc.aggregations.sum;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.IAggregation;
import lombok.extern.slf4j.Slf4j;

/**
 * A `SUM` {@link IAggregation}, managing not Number as error objects. As long as we encounter summable input, we do
 * sum. Else we aggregate `errors` into a Set.
 */
// https://learn.microsoft.com/en-us/dax/sum-function-dax
@Slf4j
public class SumElseSetAggregator extends SumAggregator {

	public static final String KEY = "SUM_ELSE_SET";

	@Override
	protected Object aggregateObjects(Object l, Object r) {
		// BEWARE This Set of errors may grow very large
		Set<Object> errors = new HashSet<>();

		addErrorsToSet(l, errors);
		addErrorsToSet(r, errors);

		if (errors.isEmpty()) {
			// This should never happen. Still, we if receive summable inputs, the errorsSet should be empty
			return null;
		}

		return errors;
	}

	@Override
	protected Object onlyOne(Object r) {
		if (r == null) {
			return null;
		} else if (isDoubleLike(r)) {
			return r;
		} else if (r instanceof Collection<?> asCollection) {
			return asCollection.stream().filter(Objects::nonNull).collect(Collectors.toSet());
		} else {
			// Wrap into a Set, so this aggregate function return either a long/double, or a Set of errors
			return Set.of(r);
		}
	}

	private static void addErrorsToSet(Object l, Set<Object> errors) {
		if (l != null && !isDoubleLike(l)) {
			if (l instanceof Collection<?> lAsSet) {
				errors.addAll(lAsSet);
			} else {
				errors.add(l);
			}
		}
	}
}
