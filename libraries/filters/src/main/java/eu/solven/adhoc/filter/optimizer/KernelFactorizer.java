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
package eu.solven.adhoc.filter.optimizer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps simplify {@link ISliceFilter} by detecting common patterns. Typically turning `(a1|b1)&(a1|b2)` into
 * `a1&(b1|b2)`.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public class KernelFactorizer {

	private static final int KERNEL_WARN_ITERATIONS = 1024;
	private static int kernelMaxIterations = 0;

	protected final FilterOptimizer filterOptimizer;

	public static void resetUnsafe() {
		kernelMaxIterations = 0;
	}

	/**
	 * Dedicated to turn `(a==a1|b==b1)&(a==a1)` to `(a==a1)`.
	 * 
	 * TODO turn `!(a==a1&b==b1)&(a!=a1)` to `(a!=a1)`.
	 * 
	 * @param andOperands
	 *            the input, consisting in a Collection of operands being `AND` together.
	 * @return
	 */
	// https://en.m.wikipedia.org/wiki/Logic_optimization
	// https://en.m.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm
	// https://en.m.wikipedia.org/wiki/Espresso_heuristic_logic_minimizer
	// BEWARE This algorithm is not smart at all, as it catches only a very limited number of cases.
	// One may contribute a finer implementation.
	protected ImmutableSet<? extends ISliceFilter> optimizeAndOfOr(ImmutableSet<? extends ISliceFilter> andOperands) {
		// 1- Segment filters between OR-like filters and others
		// OR-like filters will be combined with a cartesian product, in the hope of rejecting many irrelevant
		// combinations. The others are combined as a single AND.

		// Group by operand which may interact with others, as an OR can not be simplified if its column does not appear
		// anywhere else
		Map<Boolean, ImmutableSet<ISliceFilter>> ignorableToOperands =
				filterOptimizer.partitionByPotentialInteraction(andOperands);

		// Group by operands which are splitable by OR, as we hope to detect combinations which are useless
		Map<Boolean, List<ISliceFilter>> orNotOr = andOperands.stream()
				.collect(Collectors.partitioningBy(
						f -> filterOptimizer.hasOrOperands(f) && ignorableToOperands.get(false).contains(f)));

		List<ISliceFilter> orOperands = orNotOr.get(true);
		if (orOperands.isEmpty()) {
			// There is no OR : this is a plain AND.
			return andOperands;
		} else if (orOperands.size() == 1) {
			// There is no cartesian product: this is a plain AND.
			return andOperands;
		}

		// Holds all AND operands which are not splittable as ORs.
		ISliceFilter where = filterOptimizer.optimizeOperand(FilterBuilder.and(orNotOr.get(false)).combine(), false);

		if (where.isMatchNone()) {
			// For some reason, this was not detected earlier
			return ImmutableSet.of(ISliceFilter.MATCH_NONE);
		}

		// Given `a&b|c&d`, it may be turned into `!(a&b)&!(c&d)` then into `(!a|!b)&(!c|!d)`
		// It means we turned a simple OR into a 4-entries cartesianProduct. It indicates a simple large OR
		// would be turned into a huge cartesianProduct

		// Register if at least one simplification occurred. If false, this is useful not to later call `Or.or`
		// which would lead to a cycle as `Or.or` is based on `And.and` which is based on current method.
		AtomicBoolean hasSimplified = new AtomicBoolean();

		// Each OR-like is split into OR operands, and they are stripped individually given the common `AND` operand.
		// OR operands will be combined (through cartesian product) and cross-stripped in a later step, as the goal here
		// is to reduce the cartesian product as much as possible.
		ImmutableSet<ISliceFilter> strippedOrFiltersAsAnd =
				filterOptimizer.splitAndStripOrs(hasSimplified, where, orOperands);

		// Factor out common sub-expressions (kernels) across the OR operands before the cartesian product.
		// E.g. `(K|A) & (K|B) & (K|C) & D` → `(K|(A&B&C)) & D`, reducing the cartesian product size.
		ImmutableSet<ISliceFilter> kernelExtracted = kernelsRefactoring(strippedOrFiltersAsAnd);

		// Consider skipping the cartesianProduct given other optimizations
		if (!filterOptimizer.withCartesianProductsAndOr) {
			if (ISliceFilter.MATCH_ALL.equals(where)) {
				return kernelExtracted;
			} else {
				return ImmutableSet.<ISliceFilter>builder().add(where).addAll(kernelExtracted).build();
			}
		}

		return optimizeAndOrCartesianProduct(where, kernelExtracted);
	}

	/**
	 * Factors out common sub-expressions (kernels) shared by multiple OR operands in an AND-of-ORs expression.
	 * <p>
	 * Applies the Boolean identity {@code (K|A) & (K|B) & ... & (K|An) = K | (A&B&...&An)} greedily: at each iteration
	 * the kernel (non-OR sub-operand) that appears in the greatest number of OR operands is extracted, replacing those
	 * n OR operands with the single combined OR operand {@code K | AND(remainders)}. This shrinks the cartesian product
	 * computed by the subsequent step.
	 * <p>
	 * Only non-OR sub-operands are considered as kernel candidates to keep the cost of detection low.
	 *
	 * @param orAndOperands
	 *            the OR filters that are being AND-ed together
	 * @return an equivalent but potentially smaller set of OR filters
	 */
	@SuppressWarnings("PMD.AssignmentInOperand")
	protected ImmutableSet<ISliceFilter> kernelsRefactoring(ImmutableSet<ISliceFilter> orAndOperands) {
		List<ISliceFilter> mutableList = new ArrayList<>(orAndOperands);

		int tryIndex = 0;

		FilterUtility filterUtility = FilterUtility.builder().optimizer(filterOptimizer).build();
		do {
			if (tryIndex > KERNEL_WARN_ITERATIONS && tryIndex > kernelMaxIterations
					&& Integer.bitCount(tryIndex) == 1) {
				// Some log, helping to report slowness, and potentially live-lock
				log.warn("Kernel extraction is going deep. tryIndex={}", tryIndex);
			}

			// Map each non-OR sub-operand (kernel candidate) to the indices of OR operands that contain it
			Map<ISliceFilter, List<Integer>> kernelToIndices = new LinkedHashMap<>();
			for (int i = 0; i < mutableList.size(); i++) {
				for (ISliceFilter subOp : filterOptimizer.getOrOperands(mutableList.get(i))) {
					kernelToIndices.computeIfAbsent(subOp, k -> new ArrayList<>()).add(i);
				}
			}

			// Greedily pick the kernel appearing in the most OR operands (minimum 2 to make extraction worthwhile)
			Optional<Map.Entry<ISliceFilter, List<Integer>>> bestEntry = kernelToIndices.entrySet()
					.stream()
					.filter(e -> e.getValue().size() >= 2)
					.max(Comparator.comparingInt(e -> e.getValue().size()));

			if (bestEntry.isEmpty()) {
				break;
			}

			// ISliceFilter kernel = bestEntry.get().getKey();
			List<Integer> indices = bestEntry.get().getValue();

			ImmutableSet<ISliceFilter> withCommonOr =
					indices.stream().map(mutableList::get).collect(ImmutableSet.toImmutableSet());

			// This will include the initial `kernel`, but also the largest kernel.
			ISliceFilter commonOr = filterUtility.commonOr(withCommonOr);

			// Build the AND of remainders: each matching OR operand with the kernel removed
			ImmutableSet<ISliceFilter> remainders = indices.stream().map(i -> {
				ISliceFilter originalOperand = mutableList.get(i);
				return FilterHelpers
						.simplifyOrGivenContribution(filterOptimizer.filterStripperFactory, commonOr, originalOperand);
			}).collect(ImmutableSet.toImmutableSet());

			// Needs to be optimize as it may be a matchNone
			ISliceFilter andOfRemainders = filterOptimizer.and(remainders, false);

			// No need to optimize as we will `splitOr` right away
			ISliceFilter newOrOperand = FilterBuilder.or(commonOr, andOfRemainders).combine();

			// Remove the original OR operands in reverse index order to preserve correct positions
			List<Integer> sortedIndices = new ArrayList<>(indices);
			sortedIndices.sort(Comparator.reverseOrder());
			sortedIndices.forEach(i -> mutableList.remove((int) i));
			mutableList.add(newOrOperand);

		} while (tryIndex++ >= 0);

		onKernelFactoring(tryIndex);

		return ImmutableSet.copyOf(mutableList);
	}

	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	protected void onKernelFactoring(int tryIndex) {
		synchronized (FilterOptimizer.class) {
			kernelMaxIterations = Math.max(tryIndex, kernelMaxIterations);
		}
	}

	@Deprecated(since = "CartesianProduct seems useless since Kernel Factorization")
	protected ImmutableSet<? extends ISliceFilter> optimizeAndOrCartesianProduct(ISliceFilter where,
			ImmutableSet<ISliceFilter> andOperands) {
		List<List<ISliceFilter>> strippedOrFilters = andOperands.stream()
				.map(filterOptimizer::getOrOperands)
				.<List<ISliceFilter>>map(ImmutableList::copyOf)
				.toList();

		// This method prevents `Lists.cartesianProduct` to throw if the cartesianProduct is larger than
		// Integer.MAX_VALUE
		BigInteger cartesianProductSize = AdhocCollectionHelpers.cartesianProductSize(strippedOrFilters);

		// If the cartesian product is too large, it is unclear if we prefer to fail, or to skip the optimization
		// Skipping the optimization might lead to later issue, preventing recombination of CubeQueryStep
		if (cartesianProductSize.compareTo(BigInteger.valueOf(AdhocUnsafe.cartesianProductLimit)) > 0) {
			filterOptimizer.listener.onSkip(AndFilter.builder().ands(andOperands).build());
			log.warn("Skip .optimizeAndOfOr due to {} > {} (input={})",
					cartesianProductSize,
					AdhocUnsafe.cartesianProductLimit,
					andOperands);
			// throw new NotYetImplementedException("Faulty optimization on %s".formatted(operands));
			return andOperands;
		}

		// Holds the set of flatten entries (e.g. given `(a|b)&(c|d)`, it holds `a&c`, `a&d`, `b&c` and `b&d`).
		// BEWARE Do we rely want to do a cartesianProduct if case of an `IN` with a large number of operands?
		List<List<ISliceFilter>> cartesianProduct = Lists.cartesianProduct(strippedOrFilters);

		Set<ISliceFilter> ors = cartedianProductAndStripOrs(where, cartesianProduct);
		if (ors.size() < cartesianProductSize.intValueExact()) {
			// At least one simplification occurred: it is relevant to build and optimize an OR over the leftover
			// operands

			// BEWARE This heuristic is very weak. We should have a clearer score to define which expressions is
			// better/simpler/faster. Generally speaking, we prefer AND over OR.
			// BEWARE We go into another batch of optimization for OR, which is safe as this is strictly simpler than
			// current input
			ISliceFilter orCandidate = filterOptimizer.and(ImmutableList.of(where, OrFilter.copyOf(ors)), false);
			long costSimplifiedOr = filterOptimizer.costFunction.cost(orCandidate);
			long costInputAnd = filterOptimizer.costFunction.cost(andOperands);

			if (costSimplifiedOr < costInputAnd) {
				return ImmutableSet.copyOf(filterOptimizer.splitAnd(ImmutableSet.of(orCandidate)));
			}
		}

		// raw AND as we do not want to call optimizations recursively
		return andOperands;
	}

	@Deprecated(since = "CartesianProduct seems useless since Kernel Factorization")
	protected Set<ISliceFilter> cartedianProductAndStripOrs(ISliceFilter commonAnd,
			List<List<ISliceFilter>> cartesianProduct) {
		return cartesianProduct.stream()
				.map(t -> filterOptimizer.and(t, false))
				.filter(sf -> !sf.isMatchNone())
				// Combine the simple AND (based on not OR operands) with the orEntry.
				// Keep the simple AND on the left, as they are common to all entries, hence easier to be read
				// if first
				// Some entries would be filtered out
				// e.g. given `(a|b)&(!a|c)`, the entry `a&!a` isMatchNone
				.filter(sf -> {
					// TODO if `commonAnd` is matchAll, we can skip `.optimize`
					ISliceFilter combinedOrOperand = filterOptimizer.and(ImmutableList.of(commonAnd, sf), false);

					if (combinedOrOperand.isMatchNone()) {
						// Reject this OR which is irrelevant
						// (e.g. flattening `AND(OR(...))`, it generated a filter like `a&!a`)
						return false;
					} else {
						return true;
					}
				})
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}
}
