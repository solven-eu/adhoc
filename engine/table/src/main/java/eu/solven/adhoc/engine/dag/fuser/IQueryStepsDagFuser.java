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
package eu.solven.adhoc.engine.dag.fuser;

import eu.solven.adhoc.engine.QueryStepsDag;

/**
 * Pluggable optimizer pass over an in-progress query-steps DAG. {@code QueryStepsDagBuilder} threads the configured
 * fuser chain over the DAG after it is fully populated and before {@code QueryStepsDag} is returned.
 *
 * <p>
 * The contract is purely functional: a fuser receives an immutable {@link QueryStepsDag} and returns a possibly
 * different one. Implementations that need to mutate the graph or DAG must do so on a defensive copy and assemble a new
 * {@link QueryStepsDag} via {@link QueryStepsDag#toBuilder()}. Returning the input unchanged is the natural no-op
 * outcome when no candidate is found.
 *
 * <p>
 * Implementations must preserve the explicits (user-requested steps) and the leaves (out-degree-zero steps that the
 * table layer will materialize) — only internal structure may change. Pre-cached steps (keys of {@code stepToValues})
 * must also survive.
 *
 * <p>
 * Implementations should be idempotent: running the same fuser twice on the same DAG must produce the same result as
 * running it once.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IQueryStepsDagFuser {

	/**
	 * Apply this fuser's rewrites to {@code input} and return the result.
	 *
	 * @param input
	 *            the DAG before this fuser ran
	 * @return the DAG after this fuser ran. Returning {@code input} unchanged is allowed when no rewrite applied.
	 */
	QueryStepsDag fuse(QueryStepsDag input);
}
