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
package eu.solven.adhoc.query.filter.optimizer;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterUtility;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
import eu.solven.adhoc.util.AdhocTime;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enables `AND`, `OR` and `NOT` operations by optimizing eagerly the output. It is especially useful to have uniform
 * representation of given filters, especially if they are used as key in hash-structures.
 * 
 * Typical usecase is splitting two filters given their common fraction (e.g. `f1:a=a1&b=b1` and `f2:a=a1&c=c1` may be
 * turned into the common `c:a=a1` and complementary two filters `f1_:b=b1` and `f2_:c=c1`). We would later recombine
 * the common fraction with the complementary filters, and we may need to make sure it gives the same original filters.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings("PMD.GodClass")
@SuperBuilder
public class FilterOptimizer implements IFilterOptimizer {
	@Default
	final IOptimizerEventListener listener = new IOptimizerEventListener() {

	};

	@Default
	final IFilterCostFunction costFunction = new StandardFilterCostFunction();

	final boolean withCartesianProductsAndOr;

	@Override
	public ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		AndFilter filterForEvent = AndFilter.builder().ands(filters).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.clock);
		try {
			return notCachedAnd(filters, willBeNegated);
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	@Override
	public ISliceFilter or(Collection<? extends ISliceFilter> filters) {
		OrFilter filterForEvent = OrFilter.builder().ors(filters).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.clock);
		try {
			return notCachedOr(filters);
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	@Override
	public ISliceFilter not(ISliceFilter filter) {
		NotFilter filterForEvent = NotFilter.builder().negated(filter).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.clock);
		try {
			return notCachedNot(filter);
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	protected ISliceFilter notCachedAnd(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		// First, we ensure operands are themselves optimized. This may lead to duplicate work, as later step may
		// themselves optimize. But it is useful to ensure consistency of equivalent inputs.
		ImmutableSet<? extends ISliceFilter> optimizedOperands = optimizeOperands(filters);

		// We need to start by flattening the input (e.g. `AND(AND(a=a1,b=b2)&a=a2)` to `AND(a=a1,b=b2,a=a2)`)
		ImmutableSet<? extends ISliceFilter> flatten = splitAnd(optimizedOperands);

		// Normalization refers to grouping columns together, and discarding irrelevant operands
		// Do it before cartesianProduct, to simplify the cartesianProduct
		ImmutableSet<? extends ISliceFilter> stripRedundancyPre = normalizeOperands(flatten);
		if (stripRedundancyPre.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (stripRedundancyPre.size() == 1) {
			return stripRedundancyPre.iterator().next();
		}

		// TableQueryOptimizer.canInduce would typically do an `AND` over an `OR`
		// So we need to optimize `(c=c1) AND (c=c1 OR d=d1)`
		ImmutableSet<? extends ISliceFilter> postCartesianProduct = optimizeAndOfOr(stripRedundancyPre);

		// Normalize again after the cartesianProduct
		// TODO Skip if cartesianProduct had no effect
		ImmutableSet<? extends ISliceFilter> stripRedundancyPost = normalizeOperands(postCartesianProduct);
		if (stripRedundancyPost.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (stripRedundancyPost.size() == 1) {
			return stripRedundancyPost.iterator().next();
		}

		if (stripRedundancyPost.size() >= 2) {
			// `!(a==a1&b==b1)&!(a==a1&b==b2)&!(a==a1&b==b3)` can be turned into `a!=a1|b=out=(b1,b2,b3)`
			ISliceFilter commonOr = FilterUtility.builder().optimizer(this).build().commonOr(stripRedundancyPost);

			if (!ISliceFilter.MATCH_NONE.equals(commonOr)) {
				List<ISliceFilter> toAnd = stripRedundancyPost.stream()
						.map(f -> FilterHelpers.simplifyOrGivenContribution(commonOr, f))
						.toList();

				ISliceFilter others = and(toAnd, false);
				if (others.isMatchNone()) {
					// Special branch as the other branch does `.combine` which skip optimizations
					// Happens on `(a|b==b1)&(a|b==b2)`, which is `a|b==b1&b==b2`, which is `a`
					return commonOr;
				} else {
					return FilterBuilder.or(commonOr, others).combine();
				}

			}
		}

		return preferNotOrOverAndNot(willBeNegated, stripRedundancyPost);
	}

	protected ImmutableSet<? extends ISliceFilter> normalizeOperands(ImmutableSet<? extends ISliceFilter> flatten) {
		ImmutableSet<? extends ISliceFilter> simplifiedGivenothers = simplifyEachGivenOthers(flatten);

		// Then, we simplify given columnFilters (e.g. `a=a1&a=a2` to `matchNone`)
		ImmutableSet<? extends ISliceFilter> packedColumns = packColumnFilters(splitAnd(simplifiedGivenothers));

		if (packedColumns.contains(ISliceFilter.MATCH_NONE)) {
			return ImmutableSet.of(ISliceFilter.MATCH_NONE);
		}

		// given `a&b&c`, remove `c` if it is laxer than `a`.
		return removeLaxerInAnd(packedColumns);
	}

	// given `a==a1&(a!=a1|b==b2)`, gives `a==a1&b==b2`
	protected ImmutableSet<? extends ISliceFilter> simplifyEachGivenOthers(
			ImmutableSet<? extends ISliceFilter> andOperands) {
		if (andOperands.size() <= 1) {
			return andOperands;
		}

		// The input as a List of `AND`. This list will be mutated in place
		// BEWARE
		List<ISliceFilter> asAnds = new ArrayList<>(splitAnd(andOperands));

		for (int i = 0; i < asAnds.size(); i++) {
			ISliceFilter oneToSimplify = asAnds.get(i);

			List<ISliceFilter> contextAsAnds = new ArrayList<>(asAnds);
			contextAsAnds.remove(i);

			// `.combine` to break recursivity leading to a factorial number of combinations
			ISliceFilter simpler =
					FilterHelpers.stripWhereFromFilter(FilterBuilder.and(contextAsAnds).combine(), oneToSimplify);

			// Optimize as `stripWhereFromFilter` may break some optimization
			// Typically, `a=in=(a1,a2,a4)|b=in=(b1,b2,b4)` would be turned into `a==a1|a==a2|b==b1|b==b2` if a4 and b4
			// are not relevant.
			// It will help following `.stripWhereFromFilter`
			simpler = optimizeOperand(simpler);

			// `simpler` is simpler or same to the original expression: let's register it without trying it to compare
			// with the original expression.
			asAnds.set(i, simpler);
		}

		return ImmutableSet.copyOf(asAnds);
	}

	/**
	 * Apply recursively the optimization process to operands.
	 * 
	 * @param filters
	 * @return a {@link Set} of operands, guaranteed to be optimized.
	 */
	protected ImmutableSet<? extends ISliceFilter> optimizeOperands(Collection<? extends ISliceFilter> filters) {
		return filters.stream().map(this::optimizeOperand).collect(ImmutableSet.toImmutableSet());
	}

	protected ISliceFilter optimizeOperand(ISliceFilter f) {
		if (f instanceof IAndFilter andFilter) {
			return and(andFilter.getOperands(), false);
		} else if (f instanceof IOrFilter orFilter) {
			return or(orFilter.getOperands());
		} else if (f instanceof INotFilter notFilter) {
			return not(notFilter.getNegated());
		} else {
			// BEWARE Should we have optimizations for IColumnFilter/IValueMatchers?
			return f;
		}
	}

	protected ISliceFilter notCachedOr(Collection<? extends ISliceFilter> filters) {
		// OR relies on AND optimizations
		List<ISliceFilter> negated = filters.stream().map(this::not).toList();

		ISliceFilter negatedOptimized = and(negated, true);

		return not(negatedOptimized);
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

		Map<Boolean, List<ISliceFilter>> orNotOr =
				andOperands.stream().collect(Collectors.partitioningBy(this::hasOrOperands));

		List<ISliceFilter> orOperands = orNotOr.get(true);
		if (orOperands.isEmpty()) {
			// There is no OR : this is a plain AND.
			return andOperands;
		} else if (orOperands.size() == 1) {
			// There is no cartesian product: this is a plain AND.
			return andOperands;
		}

		// Holds all AND operands which are not splittable as ORs.
		ISliceFilter where = optimizeOperand(FilterBuilder.and(orNotOr.get(false)).combine());

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
		Set<ISliceFilter> strippedOrFiltersAsAnd = splitAndStripOrs(hasSimplified, where, orOperands);
		List<List<ISliceFilter>> strippedOrFilters = strippedOrFiltersAsAnd.stream()
				.map(this::getOrOperands)
				.<List<ISliceFilter>>map(ImmutableList::copyOf)
				.toList();

		// Consider skipping the cartesianProduct given other optimizations
		if (!withCartesianProductsAndOr) {
			return ImmutableSet.<ISliceFilter>builder().add(where).addAll(strippedOrFiltersAsAnd).build();
		}

		// This method prevents `Lists.cartesianProduct` to throw if the cartesianProduct is larger than
		// Integer.MAX_VALUE
		BigInteger cartesianProductSize = AdhocCollectionHelpers.cartesianProductSize(strippedOrFilters);

		// If the cartesian product is too large, it is unclear if we prefer to fail, or to skip the optimization
		// Skipping the optimization might lead to later issue, preventing recombination of CubeQueryStep
		if (cartesianProductSize.compareTo(BigInteger.valueOf(AdhocUnsafe.cartesianProductLimit)) > 0) {
			listener.onSkip(AndFilter.builder().ands(andOperands).build());
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

		Set<ISliceFilter> ors = cartedianProductAndStripOrs(where, hasSimplified, cartesianProduct);
		if (hasSimplified.get()) {
			// At least one simplification occurred: it is relevant to build and optimize an OR over the leftover
			// operands

			// BEWARE This heuristic is very weak. We should have a clearer score to define which expressions is
			// better/simpler/faster. Generally speaking, we prefer AND over OR.
			// BEWARE We go into another batch of optimization for OR, which is safe as this is strictly simpler than
			// current input
			ISliceFilter orCandidate = and(List.of(where, or(ors)), false);
			long costSimplifiedOr = costFunction.cost(orCandidate);
			long costInputAnd = costFunction.cost(andOperands);

			if (costSimplifiedOr < costInputAnd) {
				return ImmutableSet.copyOf(FilterHelpers.splitAnd(orCandidate));
			}
		}

		// raw AND as we do not want to call optimizations recursively
		return andOperands;
	}

	protected Set<ISliceFilter> cartedianProductAndStripOrs(ISliceFilter commonAnd,
			AtomicBoolean hasSimplified,
			List<List<ISliceFilter>> cartesianProduct) {
		return cartesianProduct.stream()
				.map(t -> and(t, false))
				// Combine the simple AND (based on not OR operands) with the orEntry.
				// Keep the simple AND on the left, as they are common to all entries, hence easier to be read
				// if first
				// Some entries would be filtered out
				// e.g. given `(a|b)&(!a|c)`, the entry `a&!a` isMatchNone
				.filter(sf -> {
					// TODO if `commonAnd` is matchAll, we can skip `.optimize`
					ISliceFilter combinedOrOperand = and(List.of(commonAnd, sf), false);

					if (combinedOrOperand.isMatchNone()) {
						// Reject this OR which is irrelevant
						// (e.g. flattening `AND(OR(...))`, it generated a filter like `a&!a`)
						hasSimplified.set(true);
						return false;
					} else {
						return true;
					}
				})
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * 
	 * @param where
	 *            a common AND operand
	 * @param andOperands
	 *            an additional {@link List} of `AND`operands which should be splittable into a bunch of OR operands.
	 * @param hasSimplified
	 *            a flag to raise when any stripping actually occurred. The goal is to detect if this methodology is
	 *            triggering or not, to prevent cycles.
	 * @return a {@link List} of AND operands
	 */
	protected Set<ISliceFilter> splitAndStripOrs(AtomicBoolean hasSimplified,
			ISliceFilter where,
			List<ISliceFilter> andOperands) {
		Collection<ISliceFilter> outputOperands = andOperands.stream()
				.map(this::getOrOperands)
				// Simplify orOperands given `WHERE`
				.map(orOperands -> splitThenStripOrs(hasSimplified, where, orOperands))
				.map(this::or)
				.collect(Collectors.toCollection(ArrayList::new));

		return removeLaxerInAnd(outputOperands);
	}

	/**
	 * 
	 * @param hasSimplified
	 * @param where
	 * @param orOperands
	 * @return a {@link List} of operands to OR, equivalent to the input {@link List}
	 */
	protected Set<ISliceFilter> splitThenStripOrs(AtomicBoolean hasSimplified,
			ISliceFilter where,
			Set<ISliceFilter> orOperands) {
		List<ISliceFilter> strippedWhere = orOperands.stream()
				.map(f -> FilterHelpers.stripWhereFromFilter(where, f))
				// Filter the combinations which are simplified into matchNone
				.filter(orOperand -> {
					ISliceFilter combinedOrOperand = and(List.of(where, orOperand), false);

					boolean matchNone = combinedOrOperand.isMatchNone();
					if (matchNone) {
						// Reject this operand which is irrelevant
						hasSimplified.set(true);
					}

					return !matchNone;
				})
				.collect(Collectors.toCollection(ArrayList::new));

		// Simplify the OR before doing the cartesian product
		int sizeBefore = strippedWhere.size();
		Set<ISliceFilter> strippedStricter = removeStricterInOr(strippedWhere);
		if (strippedWhere.size() < sizeBefore) {
			hasSimplified.set(true);
		}

		return strippedStricter;
	}

	protected ImmutableSet<ISliceFilter> removeLaxerInAnd(Collection<? extends ISliceFilter> operands) {
		return removeLaxerOrStricter(operands, true);
	}

	/**
	 * Given laxer operand imply stricter operands in OR, this will strip stricter operands.
	 * 
	 * @param operands
	 *            operands which are ORed.
	 */
	protected Set<ISliceFilter> removeStricterInOr(Collection<? extends ISliceFilter> operands) {
		return removeLaxerOrStricter(operands, false);
	}

	/**
	 * 
	 * @param operands
	 *            if `removeLaxerElseStricter==true`, this represents the operands of an AND.
	 * @param removeLaxerElseStricter
	 * @return
	 */
	protected ImmutableSet<ISliceFilter> removeLaxerOrStricter(Collection<? extends ISliceFilter> operands,
			boolean removeLaxerElseStricter) {
		Map<Boolean, List<ISliceFilter>> ignorableToOperands = partitionByPotentialInteraction(operands);

		List<ISliceFilter> toIgnore = ignorableToOperands.get(true);
		List<ISliceFilter> toProcess = ignorableToOperands.get(false);

		// Remove the operands which are induced by 1 other operand
		ImmutableSet<ISliceFilter> strippedAgainst1 = removeLaxerOrStricterGivenOne(removeLaxerElseStricter, toProcess);

		Collection<ISliceFilter> strippedAgainstAll =
				removeLaxerOrStricterGivenAll(removeLaxerElseStricter, strippedAgainst1);

		return ImmutableSet.<ISliceFilter>builder().addAll(toIgnore).addAll(strippedAgainstAll).build();
	}

	protected Map<Boolean, List<ISliceFilter>> partitionByPotentialInteraction(
			Collection<? extends ISliceFilter> operands) {
		AtomicLongMap<String> columnToNbOperands = AtomicLongMap.create();

		operands.forEach(filter -> {
			FilterHelpers.getFilteredColumns(filter).forEach(column -> {
				columnToNbOperands.incrementAndGet(column);
			});
		});

		return operands.stream().collect(Collectors.partitioningBy(operand -> {
			ISliceFilter o = operand;
			Set<String> columns = FilterHelpers.getFilteredColumns(o);

			for (String column : columns) {
				if (columnToNbOperands.get(column) != 1L) {
					// This operand may interact with another operand
					return false;
				}
			}

			return true;
		}));
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected Collection<ISliceFilter> removeLaxerOrStricterGivenAll(boolean removeLaxerElseStricter,
			Set<ISliceFilter> stripped1By1) {

		// The following phase is applied only if at least 3 operands, else previous step would have stripped it
		if (stripped1By1.size() < 3) {
			return stripped1By1;
		}

		// Remove the operands which are induced by the other operands
		// This second passes would cover the 1by1 pass. We keep at assuming it helps performances.
		List<ISliceFilter> asList = new ArrayList<>(stripped1By1);

		List<ISliceFilter> notDiscarded = new ArrayList<>();

		// Process each operand one by one to prevent cycle in the optimization process
		for (int i = 0; i < asList.size(); i++) {
			ISliceFilter mayBeDiscarded = asList.get(i);

			List<ISliceFilter> others = new ArrayList<>(stripped1By1);
			others.remove(i);

			// TODO Manage NotFilter
			if (removeLaxerElseStricter && mayBeDiscarded instanceof IOrFilter orMayBeDiscarded) {
				// Do not `FilterBuilder.and(others).optimize()` else it may lead to a huge amount of recursive
				// optimization. Given an input with N operands, this would optimize `N-1` other components, itself
				// testing `N-2` other components, etc, leading to `!N` optimizations.
				ISliceFilter otherAsAnd = FilterBuilder.and(others).combine();
				// Set<ISliceFilter> asAnd = others.stream()
				// .flatMap(f -> FilterHelpers.splitAnd(orMayBeDiscarded).stream())
				// .collect(Collectors.toSet());

				// `a&b&e&(c|a&b)` -> `a&b`
				boolean orIsImplied = orMayBeDiscarded.getOperands()
						.stream()
						// `a&b&e` is stricter than `a&b` so `(c|a&b)` is matchAll
						.anyMatch(orOperand -> FilterHelpers.isStricterThan(otherAsAnd, orOperand));

				if (orIsImplied) {
					log.trace("Discarded {} in {}", mayBeDiscarded, stripped1By1);
				} else {
					notDiscarded.add(mayBeDiscarded);
				}
			} else if (!removeLaxerElseStricter && mayBeDiscarded instanceof IAndFilter andMayBeDiscarded) {
				log.trace("TODO What's is the equivalent logic for OR? andOperands={}",
						andMayBeDiscarded.getOperands());
				// ISliceFilter otherAsAnd = FilterBuilder.and(others).optimize();
				//
				// // `a|b|e|c&!a&!b` -> `a|b`
				// boolean andIsMatchNone = andMayBeDiscarded.getOperands()
				// .stream()
				// .anyMatch(
				// andOperands -> FilterBuilder.and(others).filter(orOperand).optimize().isMatchAll());
				//
				notDiscarded.add(mayBeDiscarded);
			} else {
				notDiscarded.add(mayBeDiscarded);
			}
		}
		return notDiscarded;
	}

	// Given the list of operands, we check if at least 1 other operand implies it
	// In `AND`, `removeLaxerElseStricter` is true as `a&(a|b)==a` and `a|b` is laxer than `a`
	// In `OR`, `removeLaxerElseStricter` is false as `a|a&b==a` and `a&b` is stricter than `a`
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected ImmutableSet<ISliceFilter> removeLaxerOrStricterGivenOne(boolean removeLaxerElseStricter,
			Collection<? extends ISliceFilter> operands) {
		if (operands.size() <= 1) {
			// Need at least 2 operands to compare each other
			return ImmutableSet.copyOf(operands);
		}

		// TODO We could make a DAG of striterThan, and then iterate in topological order
		// BEWARE `FilterHelpers.isStricterThan` does not define a consistent ordering: we can not properly order a List
		// with it

		// In a first pass, we search for the laxer element, and remove for stricter
		// This suppose we often receive a list with one element covering many others
		// TODO We should do multiple passes. How many ? Until when? A DAG is the answer
		{
			List<ISliceFilter> operandsAsList = new ArrayList<>(operands);

			ISliceFilter notFinalHarder = operandsAsList.getFirst();

			for (ISliceFilter candidate : operandsAsList) {
				if (candidate != notFinalHarder) {
					if (removeLaxerElseStricter && FilterHelpers.isStricterThan(candidate, notFinalHarder)) {
						// removeLaxer, so we searcher for stricter
						notFinalHarder = candidate;
					} else if (!removeLaxerElseStricter && FilterHelpers.isLaxerThan(candidate, notFinalHarder)) {
						// removeStricter, so we search for laxer
						notFinalHarder = candidate;
					}
				}
			}
			ISliceFilter harder = notFinalHarder;

			// Remove all stricter than the laxer
			Predicate<ISliceFilter> isSofter;
			if (removeLaxerElseStricter) {
				isSofter = sf -> FilterHelpers.isLaxerThan(sf, harder);
			} else {
				isSofter = sf -> FilterHelpers.isStricterThan(sf, harder);
			}
			int sizeBefore = operandsAsList.size();
			operandsAsList.removeIf(candidate -> candidate != harder && isSofter.test(candidate));
			int sizeAfter = operandsAsList.size();
			if (sizeAfter != sizeBefore) {
				log.debug("The hardest filter suppressed {} other filters", sizeBefore - sizeAfter);
			}

			operands = operandsAsList;
		}

		long combinations = LongMath.checkedMultiply(operands.size(), operands.size() - 1);

		if (combinations > AdhocUnsafe.cartesianProductLimit) {
			if (removeLaxerElseStricter) {
				listener.onSkip(AndFilter.builder().ands(operands).build());
			} else {
				listener.onSkip(OrFilter.builder().ors(operands).build());
			}
			log.warn(
					"Skip 'removeLaxerOrStricterGivenOne' due to product={} is greater than AdhocUnsafe.cartesianProductLimit={} over {}",
					combinations,
					AdhocUnsafe.cartesianProductLimit,
					operands);
			return ImmutableSet.copyOf(operands);
		}

		// Given a list of ANDs, we reject stricter operands in profit of the laxer operands
		// Given the presence of hard, we remove soft
		Multimap<ISliceFilter, ISliceFilter> hardToSoft = MultimapBuilder.linkedHashKeys().arrayListValues().build();

		List<ISliceFilter> operandsAsList = ImmutableList.copyOf(operands);

		// This has quadratic complexity
		// In the general case, we expect only a few operands to be removed, hence we do not optimizations like those
		// implied by transitivity of strictness. (e.g. `if a>b, then we can skip looking for items owned by b as they
		// would be owned by a`)
		for (int stricterI = 0; stricterI < operandsAsList.size(); stricterI++) {
			ISliceFilter stricter = operandsAsList.get(stricterI);

			// BEWARE We need to compare `a with b` and `b with a`, as the check `isStricterThan` is not
			for (int stricterJ = 0; stricterJ < operandsAsList.size(); stricterJ++) {
				if (stricterI == stricterJ) {
					continue;
				}

				ISliceFilter laxer = operandsAsList.get(stricterJ);

				// filter induced is stricter than inducer
				if (FilterHelpers.isStricterThan(stricter, laxer)) {
					// BEWARE a laxer may have multiple stricter
					ISliceFilter hard;
					ISliceFilter soft;

					if (removeLaxerElseStricter) {
						// AND: we remove laxer filter as they are covered by stricter
						hard = stricter;
						soft = laxer;
					} else {
						// OR: we remove stricter filter as they are covered by laxer
						hard = laxer;
						soft = stricter;
					}
					hardToSoft.put(hard, soft);
				}
			}
		}

		Set<ISliceFilter> stripped = new LinkedHashSet<>(operands);

		stripped.removeAll(ImmutableSet.copyOf(hardToSoft.values()));

		return ImmutableSet.copyOf(stripped);
	}

	/**
	 * 
	 * @param f
	 * @return true if this can be expressed as a OR with at least 2 operands.
	 */
	protected boolean hasOrOperands(ISliceFilter f) {
		return FilterHelpers.splitOr(f).size() >= 2;
	}

	protected Set<ISliceFilter> getOrOperands(ISliceFilter f) {
		return FilterHelpers.splitOr(f);
	}

	// Like `and` but skipping the optimization. May be useful for debugging
	protected ImmutableSet<? extends ISliceFilter> splitAnd(ImmutableSet<? extends ISliceFilter> filters) {
		return ImmutableSet.copyOf(FilterHelpers.splitAnd(filters));
	}

	/**
	 * BEWARE This method is quite dangerous. Given a {@link ISliceFilter} is used as key in hashed structure, and
	 * {@link ISliceFilter} may be recombined through operations (e.g. split between `WHERE` and `FILTER` which are
	 * later `AND`-ed together). Hence, we need to have a single representation per equivalent boolean expression.
	 * 
	 * Using a cost-function is seductive, but it may lead to issues. Typically, if two equivalent representations has
	 * the same cost, we must also pick the same representation.
	 * 
	 * @param willBeNegated
	 * @param and
	 * @return
	 */
	protected ISliceFilter preferNotOrOverAndNot(boolean willBeNegated, Set<? extends ISliceFilter> and) {
		// Consider returning a `!(a|b)` instead of `!a&!b`
		ISliceFilter orCandidate = OrFilter.builder().ors(and.stream().map(this::not).toList()).build();
		ISliceFilter notOrCandidate = NotFilter.builder().negated(orCandidate).build();

		long costOrCandidate;
		long costAndCandidate;
		if (willBeNegated) {
			costOrCandidate = costFunction.cost(orCandidate);
			costAndCandidate = costFunction.cost(NotFilter.builder().negated(FilterBuilder.and(and).combine()).build());
		} else {
			costOrCandidate = costFunction.cost(notOrCandidate);
			costAndCandidate = costFunction.cost(and);
		}
		// BEWARE If same cost, we prefer `AND`
		if (costOrCandidate < costAndCandidate) {
			return notOrCandidate;
		}

		return FilterBuilder.and(and).combine();
	}

	/**
	 * Helps packColumnFilters
	 * 
	 * @author Benoit Lacelle
	 */
	@Builder
	protected static class PackingColumns {
		final IFilterOptimizer optimizer;

		// If there is not a single valueMatcher with explicit values, we should add all as not managed
		final AtomicBoolean hadSomeAllowedValues = new AtomicBoolean();
		// allowedValues is meaningful only if hadSomeAllowedValues is true
		final Set<Object> allowedValues = new LinkedHashSet<>();

		// If there is not a single valueMatcher with explicit values, we should add all as not managed
		final AtomicBoolean hadSomeDisallowedValues = new AtomicBoolean();
		// allowedValues is meaningful only if hadSomeAllowedValues is true
		final Set<Object> disallowedValues = new LinkedHashSet<>();

		// Do no collect IValueMatcher until managing `nullIfAbsent`
		final List<IColumnFilter> implicitFilters = new ArrayList<>();

		// This flag is used to prevent re-ordering if the filter is not actually simplified
		final AtomicBoolean hasSimplified = new AtomicBoolean();

		protected void registerPackedColumn(AtomicBoolean isMatchNone, IColumnFilter columnFilter) {
			if (columnFilter.getValueMatcher() instanceof EqualsMatcher equalsMatcher) {
				hadSomeAllowedValues.set(true);
				Object operand = equalsMatcher.getWrapped();

				addOperandInSet(isMatchNone, operand);
			} else if (columnFilter.getValueMatcher() instanceof InMatcher inMatcher) {
				hadSomeAllowedValues.set(true);
				Set<?> operands = inMatcher.getOperands();

				addOperandInSet(isMatchNone, operands);
			} else if (columnFilter.getValueMatcher() instanceof NotMatcher notMatcher) {
				if (notMatcher.getNegated() instanceof EqualsMatcher equalsMatcher) {
					hadSomeDisallowedValues.set(true);
					Object operand = equalsMatcher.getWrapped();

					disallowedValues.add(operand);
				} else if (notMatcher.getNegated() instanceof InMatcher inMatcher) {
					hadSomeDisallowedValues.set(true);
					Set<?> operands = inMatcher.getOperands();

					disallowedValues.addAll(operands);
				} else if (implicitFilters.contains(columnFilter.negate())) {
					isMatchNone.set(true);
				} else {
					implicitFilters.add(columnFilter);
				}
			} else if (implicitFilters.contains(columnFilter.negate())) {
				isMatchNone.set(true);
			} else {
				implicitFilters.add(columnFilter);
			}
		}

		protected void addOperandInSet(AtomicBoolean isMatchNone, Set<?> operands) {
			if (allowedValues.isEmpty()) {
				// This is the first accepted values
				allowedValues.addAll(operands);
			} else {
				allowedValues.retainAll(operands);
				if (allowedValues.isEmpty()) {
					// There is 2 incompatible set
					isMatchNone.set(true);
				}
			}
		}

		protected void addOperandInSet(AtomicBoolean isMatchNone, Object operand) {
			if (allowedValues.isEmpty()) {
				// This is the first accepted values
				allowedValues.add(operand);
			} else {
				if (allowedValues.contains(operand)) {
					if (allowedValues.size() == 1) {
						// Keep the allowed value
						// Happens if we have multiple EqualsMatcher on the same operand
						log.trace("Keep the allowed value");
					} else {
						// This is equivalent to a `.retain`
						allowedValues.clear();
						allowedValues.add(operand);
					}
				} else {
					// There is 2 incompatible set
					isMatchNone.set(true);
				}
			}
		}

		protected void collectPacks(AtomicBoolean isMatchNone,
				String column,
				ImmutableList.Builder<ISliceFilter> packedFiltersBuilder,
				List<ISliceFilter> notManaged) {

			if (hadSomeAllowedValues.get()) {
				// We have a list of explicit values
				if (!implicitFilters.isEmpty()) {
					// Reject the values given the implicitFilters (e.g. regex filters)
					allowedValues.removeIf(allowedValue -> {
						// Remove any value which is not accepted by all (given AND) implicitFilters
						boolean doRemove =
								implicitFilters.stream().anyMatch(f -> !f.getValueMatcher().match(allowedValue));

						if (doRemove) {
							hasSimplified.set(true);
						}

						return doRemove;
					});
				}

				// Remove explicitly disallowed values
				if (allowedValues.removeAll(disallowedValues)) {
					hasSimplified.set(true);
				}

				if (allowedValues.isEmpty()) {
					// May happen only if implicitFilters or disallowedValues rejected all allowed values
					isMatchNone.set(true);
				} else {
					// Consider a Set of explicit values
					// implicitFilters are dropped as we checked they matched the values
					packedFiltersBuilder.add(ColumnFilter.matchIn(column, allowedValues));
				}
			} else if (hadSomeDisallowedValues.get()) {
				// Some disallowedValues but no allowedValues

				if (!implicitFilters.isEmpty()) {
					// Reject the values given the implicitFilters (e.g. regex filters)

					// No need to reject explicitly w value rejected by implicit filters
					// (e.g. `az*` would reject `qwerty` so `out(qwerty)` is irrelevant)
					disallowedValues.removeIf(disallowedValue -> {
						// If empty, return false, hence do not remove the allowed value
						boolean doRemove =
								implicitFilters.stream().noneMatch(f -> f.getValueMatcher().match(disallowedValue));

						if (doRemove) {
							hasSimplified.set(true);
						}

						return doRemove;
					});
				}

				if (!disallowedValues.isEmpty()) {
					// We have a list of explicit disallowed values but no explicit values
					packedFiltersBuilder.add(optimizer.not(ColumnFilter.matchIn(column, disallowedValues)));
				}

				notManaged.addAll(implicitFilters);
			} else {
				// neither allowed or disallowed values
				notManaged.addAll(implicitFilters);
			}
		}

	}

	/**
	 * Optimize input filters if they cover same columns. Typically, `a=a1&a=a2` can be simplified into `matchNone`.
	 * 
	 * @param filters
	 *            which are considered AND together.
	 * @return a simpler list of filters, where filters are simplified on a per-column basis.
	 */
	protected ImmutableSet<? extends ISliceFilter> packColumnFilters(ImmutableSet<? extends ISliceFilter> filters) {
		if (filters.size() <= 1) {
			return filters;
		}

		@SuppressWarnings("PMD.LinguisticNaming")
		Map<Boolean, List<ISliceFilter>> isColumnToFilters =
				filters.stream().collect(Collectors.partitioningBy(f -> f instanceof IColumnFilter));

		if (isColumnToFilters.get(true).isEmpty()) {
			// Not a single columnFilter
			return filters;
		} else {
			// isMatchNone is cross column as a single column not matching anything reject the whole filter
			AtomicBoolean isMatchNone = new AtomicBoolean();

			List<ISliceFilter> notManaged = new ArrayList<>();
			if (isColumnToFilters.containsKey(false)) {
				notManaged.addAll(isColumnToFilters.get(false));
			}

			// TODO This breaks ordering by pushing implicit filters to the end
			// A solution is not trivial: iterate through filters, adding in the output if implicit, else scanning the
			// rest for same column
			Map<String, List<IColumnFilter>> columnToFilters = isColumnToFilters.get(true)
					.stream()
					.map(f -> (IColumnFilter) f)
					// https://stackoverflow.com/questions/44675454/how-to-get-ordered-type-of-map-from-method-collectors-groupingby
					// LinkedHashMap to maintain as much as possible the initial order
					.collect(Collectors.groupingBy(IColumnFilter::getColumn, LinkedHashMap::new, Collectors.toList()));

			ImmutableList.Builder<ISliceFilter> packedFiltersBuilder = ImmutableList.<ISliceFilter>builder();
			columnToFilters.forEach((column, columnFilters) -> {
				if (isMatchNone.get()) {
					// Fail-fast
					return;
				}

				PackingColumns packingColumns = new PackingColumns(this);

				// Collect filter by allowedValue, disallowedValues and implicitFilters
				columnFilters.stream().forEach(columnFilter -> {
					if (isMatchNone.get()) {
						// Fail-fast
						return;
					}

					packingColumns.registerPackedColumn(isMatchNone, columnFilter);
				});

				packingColumns.collectPacks(isMatchNone, column, packedFiltersBuilder, notManaged);

			});

			if (isMatchNone.get()) {
				// Typically happens if we have incompatible equals/in constraints
				return ImmutableSet.of(IAndFilter.MATCH_NONE);
			} else {
				return ImmutableSet.copyOf(packedFiltersBuilder
						// Add not managed at the end for readability, as they are generally more complex
						.addAll(notManaged)
						.build());
			}
		}
	}

	protected ISliceFilter notCachedNot(ISliceFilter filter) {
		if (filter.isMatchAll()) {
			return ISliceFilter.MATCH_NONE;
		} else if (filter.isMatchNone()) {
			return ISliceFilter.MATCH_ALL;
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return notFilter.getNegated();
		} else if (filter.isColumnFilter() && filter instanceof ColumnFilter columnFilter) {
			// Prefer `c!=c1` over `!(c==c1)`
			return columnFilter.toBuilder().matching(NotMatcher.not(columnFilter.getValueMatcher())).build();
			// } else if (filter instanceof IAndFilter andFilter) {
			// return FilterBuilder.or(andFilter.getOperands().stream().map(NotFilter::not).toList()).optimize();
		} else if (filter instanceof IOrFilter orFilter) {
			// Plays optimizations given an `AND` of `NOT`s.
			// We may prefer `c!=c1&d==d2` over `!(c==c1|d!=d2)`
			return and(orFilter.getOperands().stream().map(this::not).toList(), false);
		}

		// Set<ISliceFilter> ors = FilterHelpers.splitOr(filter);
		// return FilterBuilder.and(ors.stream().map(NotFilter::not).toList()).optimize();

		return NotFilter.builder().negated(filter).build();
	}
}
