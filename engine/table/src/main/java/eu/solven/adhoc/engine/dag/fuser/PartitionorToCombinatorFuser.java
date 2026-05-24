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

import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.Partitionor;
import lombok.extern.slf4j.Slf4j;

/**
 * Rewrites {@link Partitionor}-measure steps as equivalent {@link Combinator}-measure steps when the step's groupBy
 * already covers the Partitionor's own groupBy. In that case the Partitionor degenerates to its embedded combination:
 * each partition contains exactly one slice (so the per-partition aggregation is a no-op), and the engine routes the
 * underlyings at the same granularity a Combinator would request.
 *
 * <p>
 * Intended to run BEFORE {@link CombinatorSubgraphsFuser} in a {@link CompositeDagFuser}: the rewritten Combinators
 * become eligible for chain / subgraph folding.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class PartitionorToCombinatorFuser extends AToCombinatorFuser {

	@Override
	protected boolean isCandidate(CubeQueryStep step,
			Set<CubeQueryStep> roots,
			Map<CubeQueryStep, ICuboid> stepToValue) {
		return step.getMeasure() instanceof Partitionor partitionor && !roots.contains(step)
				&& !stepToValue.containsKey(step)
				&& step.getGroupBy().getSortedColumns().containsAll(partitionor.getGroupBy().getSortedColumns());
	}

	@Override
	protected Combinator buildReplacement(IMeasure original) {
		Partitionor partitionor = (Partitionor) original;
		Combinator.CombinatorBuilder builder = Combinator.builder()
				.name(partitionor.getName())
				.combinationKey(partitionor.getCombinationKey())
				.tags(partitionor.getTags());
		for (String underlying : partitionor.getUnderlyings()) {
			builder.underlying(underlying);
		}
		for (Map.Entry<String, ?> opt : partitionor.getCombinationOptions().entrySet()) {
			builder.combinationOption(opt.getKey(), opt.getValue());
		}
		return builder.build();
	}

	@Override
	protected void logRewrite(IMeasure original) {
		log.debug("Rewrote Partitionor step {} as Combinator", original.getName());
	}
}
