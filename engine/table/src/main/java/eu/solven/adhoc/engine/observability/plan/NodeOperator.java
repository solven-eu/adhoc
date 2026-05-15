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
package eu.solven.adhoc.engine.observability.plan;

/**
 * High-level category for a {@link QueryPlanNode}, independent of the precise step type. Used by the log renderer and
 * the UI to pick an icon and a label shape. {@code OTHER} is the safe default when the engine produces a step kind we
 * have not categorized yet.
 *
 * @author Benoit Lacelle
 */
public enum NodeOperator {
	/** A {@code CubeQueryStep} — measure-side composition (combinator, dispatchor, transformator, …). */
	CUBE_STEP,
	/** A measure reference (leaf in the cube layer pointing at a named measure). */
	MEASURE_REF,
	/** A {@code TableQueryStep} / {@code TableQuery} — pre-execution wiring against the table layer. */
	TABLE_STEP,
	/**
	 * The actual table query (sql / pluggable backend). Children are typically empty; this is the leaf in execution.
	 */
	TABLE_QUERY,
	/** The composite-cube fan-out — children are {@link #SUB_CUBE_DELEGATION} nodes. */
	COMPOSITE_FANOUT,
	/**
	 * A pointer into another plan in the registry (the sub-cube's own execution). Carries a non-null
	 * {@link QueryPlanNode#getSubQueryId() subQueryId}.
	 */
	SUB_CUBE_DELEGATION,
	/** The composite-side merge step that joins sub-cube results. */
	MERGE,
	/** Catch-all for step kinds not yet categorized. */
	OTHER;
}
