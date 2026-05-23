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
package eu.solven.adhoc.engine.optimizer;

import java.util.List;
import java.util.stream.Collectors;

import eu.solven.adhoc.model.measure.Combinator;

/**
 * Builds the name of the fused {@link Combinator} produced by {@link FoldCombinatorSubgraphsOptimizer}. Receives the
 * subgraph internals in top-down (BFS) order — top first, then each foldable child.
 *
 * <p>
 * The default implementation preserves both each internal's {@code name} and its {@code combinationKey}, so the fused
 * step's name keeps enough information to identify the original measures and the operation they performed (e.g.
 * {@code ratio_postcheck[DIVIDE] ∘ current_whole[COALESCE]}). Callers wanting a different convention (e.g. fully
 * qualified names, terser format, structured JSON) can plug their own implementation through
 * {@link FoldCombinatorSubgraphsOptimizer#FoldCombinatorSubgraphsOptimizer(int, IComposedNameStrategy)}.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IComposedNameStrategy {

	/** Joiner used between the rendered fragments of each internal. */
	String JOINER = " ∘ ";

	/**
	 * Default rendering: {@code name1[KEY1] ∘ name2[KEY2] ∘ ...} where each fragment is the internal's measure name
	 * followed by its {@link Combinator#getCombinationKey()} in square brackets.
	 */
	IComposedNameStrategy DEFAULT = internals -> internals.stream()
			.map(c -> c.getName() + "[" + c.getCombinationKey() + "]")
			.collect(Collectors.joining(JOINER));

	/**
	 * @param internals
	 *            the foldable combinators about to be fused, in top-down (BFS) order — first element is the topmost
	 *            (closest to the consumer), subsequent elements are its foldable descendants.
	 * @return the name to assign to the fused {@link Combinator}.
	 */
	String name(List<Combinator> internals);
}
