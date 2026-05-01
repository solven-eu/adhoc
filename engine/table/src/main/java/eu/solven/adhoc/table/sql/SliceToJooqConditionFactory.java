/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.sql;

import java.util.function.Function;

import org.jooq.DSLContext;
import org.jooq.Name;

import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Enable {@link SliceToJooqCondition} instance given a naming policy (e.g. which depends on the {@link DSLContext}).
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class SliceToJooqConditionFactory {

	/**
	 * Creates a {@link SliceToJooqCondition} with the given naming policy and the default
	 * {@link AdhocFilterUnsafe#filterOptimizer}.
	 */
	public ISliceToJooqCondition with(Function<String, Name> toName) {
		return with(toName, AdhocFilterUnsafe.filterOptimizer);
	}

	/**
	 * Creates a {@link SliceToJooqCondition} with the given naming policy and the supplied query-scoped optimizer.
	 *
	 * @param filterOptimizer
	 *            optimizer used for all {@link eu.solven.adhoc.filter.FilterBuilder#optimize(IFilterOptimizer)} calls
	 *            inside the produced instance
	 */
	public ISliceToJooqCondition with(Function<String, Name> toName, IFilterOptimizer filterOptimizer) {
		return SliceToJooqCondition.builder().toName(toName).filterOptimizer(filterOptimizer).build();
	}
}
