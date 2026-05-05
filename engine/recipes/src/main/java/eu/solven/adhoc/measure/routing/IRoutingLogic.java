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
package eu.solven.adhoc.measure.routing;

import java.util.List;

import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * Decomposes a {@link CubeQueryStep} into one or more underlying steps. The {@link RoutingMeasure} runtime queries each
 * returned step and combines their per-slice values via SUM, so implementations should ensure the returned steps'
 * filters are disjoint when their summed contributions must equal the parent step's value (the standard cutoff-style
 * migration scenario).
 *
 * <p>
 * Implementations should be cheap and pure: the function is invoked once per parent step, and its result is treated as
 * a deterministic property of that step.
 *
 * <p>
 * Build returned steps with {@code CubeQueryStep.edit(parentStep).measure(...).filter(...).build()} so groupBy /
 * options / customMarker identity is preserved for the DAG's deduplication.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IRoutingLogic {

	/**
	 * @param step
	 *            the parent step the engine is asking this measure to compute
	 * @return a non-empty list of underlying steps whose summed per-slice values produce the output
	 */
	List<CubeQueryStep> route(CubeQueryStep step);
}
