/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.filter.optimizer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;

/**
 * Unit-tests for {@link FilterOptimizerIntraCache}: basic optimisation smoke-tests and verification that the
 * {@link FilterOptimizerWithCache} produced by {@link FilterOptimizerIntraCache#makeWithCache()} caches
 * {@link IFilterStripper} instances so that the same non-trivial filter always resolves to the same stripper instance
 * within a single optimisation pass.
 */
public class TestFilterOptimizerIntraCache {

	FilterOptimizerIntraCache optimizer = FilterOptimizerIntraCache.builder().build();

	/**
	 * Duplicate AND operands are collapsed to a single filter.
	 */
	@Test
	public void and_deduplicatesSameOperand() {
		ISliceFilter filter = ColumnFilter.matchEq("a", "a1");
		ISliceFilter combined = FilterBuilder.and(filter, filter).optimize(optimizer);
		Assertions.assertThat(combined).isEqualTo(filter);
	}

	/**
	 * A redundant OR operand that is already implied by a stricter AND clause is removed.
	 */
	@Test
	public void and_removesRedundantOrClause() {
		// a==a1 AND (a==a1 OR b==b1) → a==a1 (the OR is already covered by a==a1)
		ISliceFilter combined =
				FilterBuilder
						.and(ColumnFilter.matchEq("a", "a1"),
								FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"))
										.combine())
						.optimize(optimizer);
		Assertions.assertThat(combined).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	/**
	 * Within a single {@link FilterOptimizerWithCache} produced by {@link FilterOptimizerIntraCache#makeWithCache()},
	 * calling the inner {@link IFilterStripperFactory} with the same non-trivial {@link ISliceFilter} twice must return
	 * the <em>same</em> {@link IFilterStripper} instance. This verifies that the cache inside
	 * {@link FilterOptimizerWithCache} is wired through {@link FilterOptimizerIntraCache#makeWithCache()} and actually
	 * applies to the stripper instances it hands out.
	 */
	@Test
	public void makeWithCache_sameFilterReturnsSameStripperInstance() {
		FilterOptimizerWithCache withCache = optimizer.makeWithCache();
		IFilterStripperFactory factory = withCache.getFilterStripperFactory();

		ISliceFilter filter = ColumnFilter.matchEq("a", "a1");
		IFilterStripper s1 = factory.makeFilterStripper(filter);
		IFilterStripper s2 = factory.makeFilterStripper(filter);

		Assertions.assertThat(s1).isSameAs(s2);
	}

	/**
	 * Two different filters must produce different {@link IFilterStripper} instances (no cross-contamination of the
	 * cache).
	 */
	@Test
	public void makeWithCache_differentFiltersReturnDifferentStripperInstances() {
		FilterOptimizerWithCache withCache = optimizer.makeWithCache();
		IFilterStripperFactory factory = withCache.getFilterStripperFactory();

		IFilterStripper s1 = factory.makeFilterStripper(ColumnFilter.matchEq("a", "a1"));
		IFilterStripper s2 = factory.makeFilterStripper(ColumnFilter.matchEq("a", "a2"));

		Assertions.assertThat(s1).isNotSameAs(s2);
	}
}
