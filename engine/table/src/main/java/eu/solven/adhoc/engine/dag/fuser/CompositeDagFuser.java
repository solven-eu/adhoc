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

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.engine.QueryStepsDag;

/**
 * Runs a sequence of {@link IQueryStepsDagFuser}s in order. Each delegate's output is fed as the next delegate's input,
 * making the composite equivalent to {@code f_n ∘ … ∘ f_1}.
 *
 * <p>
 * Composition order matters: a later fuser can take advantage of earlier ones. For example, a
 * {@link PartitionorToCombinatorFuser} (turning a Partitionor whose partitioning columns are already in the groupBy
 * into a plain Combinator) should run BEFORE {@link CombinatorSubgraphsFuser} so the newly-introduced Combinators
 * participate in the chain folding.
 *
 * @author Benoit Lacelle
 */
public class CompositeDagFuser implements IQueryStepsDagFuser {

	private final List<IQueryStepsDagFuser> delegates;

	public CompositeDagFuser(IQueryStepsDagFuser... delegates) {
		this(Arrays.asList(delegates));
	}

	public CompositeDagFuser(List<? extends IQueryStepsDagFuser> delegates) {
		this.delegates = ImmutableList.copyOf(delegates);
	}

	@Override
	public QueryStepsDag fuse(QueryStepsDag input) {
		QueryStepsDag current = input;
		for (IQueryStepsDagFuser delegate : delegates) {
			current = delegate.fuse(current);
		}
		return current;
	}
}
