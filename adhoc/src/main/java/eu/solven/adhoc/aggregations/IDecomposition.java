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
package eu.solven.adhoc.aggregations;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;

/**
 * Used for {@link eu.solven.adhoc.transformers.IMeasure} which generates/contributes into multiple slices given an
 * underlying slice. It may be used for rebucketing (e.g. `123 on 0.2` may be decomposed into `123*0.2 into 0 and
 * 123*0.8 into 1.0`).
 *
 * Also used for many2many: each input element is decomposed into its target groups.
 */
public interface IDecomposition {
	/**
	 *
	 * @param slice
	 *            an element/underlying slice
	 * @param value
	 * @return the target/pillars/groups slices
	 */
	Map<Map<String, ?>, Object> decompose(IAdhocSliceWithStep slice, Object value);

	/**
	 * @param step
	 * @return the columns which MAY be written by decompositions.
	 */
	List<IWhereGroupbyAdhocQuery> getUnderlyingSteps(AdhocQueryStep step);
}
