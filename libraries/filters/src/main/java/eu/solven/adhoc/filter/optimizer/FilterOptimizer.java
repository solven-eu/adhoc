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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.math.LongMath;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.FilterUtility;
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.NotFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.util.AdhocTime;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.Getter;
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
public class FilterOptimizer implements IFilterOptimizer, IHasFilterStripperFactory {
	@Default
	final IOptimizerEventListener listener = new IOptimizerEventListener() {

	};

	@Default
	final IFilterCostFunction costFunction = new StandardFilterCostFunction();

	@Default
	@Getter
	IFilterStripperFactory filterStripperFactory = AdhocFilterUnsafe.filterStripperFactory;

	@Default
	Function<FilterOptimizer, KernelFactorizer> kernelFactorizerFactory = fo -> new KernelFactorizer(fo);

	@Deprecated(since = "CartesianProduct seems useless since Kernel Factorization")
	@Default
	final boolean withCartesianProductsAndOr = false;

	@Override
	public ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		AndFilter filterForEvent = AndFilter.builder().ands(filters).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.unsafeClock);
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

		Instant start = Instant.now(AdhocTime.unsafeClock);
		try {
			if (filters.contains(ISliceFilter.MATCH_ALL)) {
				return ISliceFilter.MATCH_ALL;
			} else if (filters.isEmpty()) {
				// Prevent caching trivial case
				return ISliceFilter.MATCH_NONE;
			} else if (filters.size() == 1) {
				// Prevent caching in OR what is trivial AND.
				return notCachedAnd(filters, false);
			} else {
				return notCachedOr(filters);
			}
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	@Override
	public ISliceFilter not(ISliceFilter filter, boolean willBeNegated) {
		NotFilter filterForEvent = NotFilter.builder().negated(filter).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.unsafeClock);
		try {
			return notCachedNot(filter, willBeNegated);
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	protected ISliceFilter notCachedAnd(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		if (filters.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (filters.contains(ISliceFilter.MATCH_NONE)) {
			// This is covered by FilterBuilder, but FilterOptimizer does not rely on it for its recursive calls
			return ISliceFilter.MATCH_NONE;
		}

		// First, we ensure operands are themselves optimized. This may lead to duplicate work, as later step may
		// themselves optimize. But it is useful to ensure consistency of equivalent inputs.
		ImmutableSet<? extends ISliceFilter> optimizedOperands = optimizeOperands(filters, willBeNegated);

		// We need to start by flattening the input (e.g. `AND(AND(a=a1,b=b2)&a=a2)` to `AND(a=a1,b=b2,a=a2)`)
		ImmutableSet<? extends ISliceFilter> flatten = splitAnd(optimizedOperands);

		// Split by independent column clusters: filters whose columns do not overlap can be optimized
		// independently, then AND-ed together. This lets the 2-element commonOr logic in
		// preferNotOrOverAndNot handle subsets that share columns without being distracted by
		// unrelated filters.
		ImmutableSet<? extends ISliceFilter> clusterResults = optimizeByClusters(flatten, willBeNegated);

		if (clusterResults.contains(ISliceFilter.MATCH_NONE)) {
			return ISliceFilter.MATCH_NONE;
		} else if (clusterResults.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		}

		return preferNotOrOverAndNot(clusterResults, willBeNegated);
	}

	protected ImmutableSet<? extends ISliceFilter> andOneCluster(Set<? extends ISliceFilter> operands,
			boolean willBeNegated) {
		// Normalization refers to grouping columns together, and discarding irrelevant operands
		// Do it before cartesianProduct, to simplify the cartesianProduct
		ImmutableSet<? extends ISliceFilter> stripRedundancyPre = normalizeOperands(operands);
		if (stripRedundancyPre.isEmpty()) {
			// matchAll
			return ImmutableSet.of();
		} else if (stripRedundancyPre.size() == 1) {
			return ImmutableSet.of(preferNotOrOverAndNot(stripRedundancyPre, willBeNegated));
		}

		// TableQueryOptimizer.canInduce would typically do an `AND` over an `OR`
		// So we need to optimize `(c=c1) AND (c=c1 OR d=d1)`
		ImmutableSet<? extends ISliceFilter> postCartesianProduct =
				kernelFactorizerFactory.apply(this).optimizeAndOfOr(stripRedundancyPre);

		// Normalize again after the cartesianProduct
		// TODO Skip if cartesianProduct had no effect
		ImmutableSet<? extends ISliceFilter> normalized = normalizeOperands(postCartesianProduct);

		return ImmutableSet.of(preferNotOrOverAndNot(normalized, willBeNegated));
	}

	protected ImmutableSet<? extends ISliceFilter> normalizeOperands(Set<? extends ISliceFilter> operands) {
		if (operands.size() <= 1) {
			return ImmutableSet.copyOf(operands);
		}

		Map<Boolean, ImmutableSet<ISliceFilter>> ignorableToOperands = partitionByPotentialInteraction(operands);

		ImmutableSet<ISliceFilter> toIgnore = ignorableToOperands.get(true);
		ImmutableSet<ISliceFilter> toProcess = ignorableToOperands.get(false);

		ImmutableSet<? extends ISliceFilter> simplifiedGivenOthers = simplifyEachGivenOthers(toProcess);

		// Then, we simplify given columnFilters (e.g. `a=a1&a=a2` to `matchNone`)
		ImmutableSet<? extends ISliceFilter> packedColumns =
				new ColumnPacker(this).packColumnFilters(splitAnd(simplifiedGivenOthers));

		if (packedColumns.contains(ISliceFilter.MATCH_NONE)) {
			return ImmutableSet.of(ISliceFilter.MATCH_NONE);
		}

		ImmutableSet<ISliceFilter> laxerRemoved = removeLaxerInAnd(packedColumns);

		// given `a&b&c`, remove `c` if it is laxer than `a`.
		return AdhocCollectionHelpers.copyOfSets(toIgnore, laxerRemoved);
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
			IFilterStripper filterStripper =
					filterStripperFactory.makeFilterStripper(FilterBuilder.and(contextAsAnds).combine());
			ISliceFilter simpler = filterStripper.strip(oneToSimplify);

			// Optimize as `stripWhereFromFilter` may break some optimization
			// Typically, `a=in=(a1,a2,a4)|b=in=(b1,b2,b4)` would be turned into `a==a1|a==a2|b==b1|b==b2` if a4 and b4
			// are not relevant.
			// It will help following `.stripWhereFromFilter`
			simpler = optimizeOperand(simpler, false);

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
	protected ImmutableSet<? extends ISliceFilter> optimizeOperands(Collection<? extends ISliceFilter> filters,
			boolean willBeNegated) {
		return filters.stream().map(f -> optimizeOperand(f, willBeNegated)).collect(ImmutableSet.toImmutableSet());
	}

	protected ISliceFilter optimizeOperand(ISliceFilter f, boolean willBeNegated) {
		if (f instanceof IAndFilter andFilter) {
			return and(andFilter.getOperands(), willBeNegated);
		} else if (f instanceof IOrFilter orFilter) {
			return or(orFilter.getOperands());
		} else if (f instanceof INotFilter notFilter) {
			return not(notFilter.getNegated(), willBeNegated);
		} else {
			// BEWARE Should we have optimizations for IColumnFilter/IValueMatchers?
			return f;
		}
	}

	// https://en.wikipedia.org/wiki/De_Morgan%27s_laws
	protected ISliceFilter notCachedOr(Collection<? extends ISliceFilter> filters) {
		// Prepare without optimization
		List<ISliceFilter> negated = filters.stream().map(ISliceFilter::negate).toList();

		// OR relies on AND optimizations
		ISliceFilter negatedOptimized = and(negated, true);

		// Unroll without optimization
		return NotFilter.simpleNot(negatedOptimized);
	}

	/**
	 * 
	 * @param where
	 *            a common AND operand
	 * @param andOperands
	 *            an additional {@link List} of `AND`operands which should be splittable into a bunch of OR operands.
	 * @return a {@link List} of AND operands
	 */
	protected ImmutableSet<ISliceFilter> splitAndStripOrs(ISliceFilter where, Set<? extends ISliceFilter> andOperands) {
		Set<? extends ISliceFilter> outputOperands;

		if (where.isMatchAll()) {
			outputOperands = andOperands;
		} else {
			outputOperands = andOperands.stream()
					.map(this::getOrOperands)
					// Simplify orOperands given `WHERE`
					.map(orOperands -> splitThenStripOrs(where, orOperands))
					.map(this::or)
					.collect(ImmutableSet.toImmutableSet());
		}

		return removeLaxerInAnd(outputOperands);
	}

	/**
	 * 
	 * @param where
	 * @param orOperands
	 * @return a {@link List} of operands to OR, equivalent to the input {@link List}
	 */
	protected Set<ISliceFilter> splitThenStripOrs(ISliceFilter where, Set<ISliceFilter> orOperands) {
		IFilterStripper filterStripper = filterStripperFactory.makeFilterStripper(where);

		Set<ISliceFilter> strippedWhere = orOperands.stream()
				.map(filterStripper::strip)
				// Filter the combinations which are simplified into matchNone
				.filter(orOperand -> {
					ISliceFilter combinedOrOperand = and(ImmutableList.of(where, orOperand), false);

					boolean matchNone = combinedOrOperand.isMatchNone();

					return !matchNone;
				})
				.collect(ImmutableSet.toImmutableSet());

		// Simplify the OR before doing the cartesian product
		return removeStricterInOr(strippedWhere);
	}

	protected ImmutableSet<ISliceFilter> removeLaxerInAnd(Set<? extends ISliceFilter> operands) {
		return removeLaxerOrStricter(operands, true);
	}

	/**
	 * Given laxer operand imply stricter operands in OR, this will strip stricter operands.
	 * 
	 * @param operands
	 *            operands which are ORed.
	 */
	protected Set<ISliceFilter> removeStricterInOr(Set<? extends ISliceFilter> operands) {
		return removeLaxerOrStricter(operands, false);
	}

	/**
	 * 
	 * @param operands
	 *            if `removeLaxerElseStricter==true`, this represents the operands of an AND.
	 * @param removeLaxerElseStricter
	 * @return
	 */
	protected ImmutableSet<ISliceFilter> removeLaxerOrStricter(Set<? extends ISliceFilter> operands,
			boolean removeLaxerElseStricter) {
		Map<Boolean, ImmutableSet<ISliceFilter>> ignorableToOperands = partitionByPotentialInteraction(operands);

		ImmutableSet<ISliceFilter> toIgnore = ignorableToOperands.get(true);
		ImmutableSet<ISliceFilter> toProcess = ignorableToOperands.get(false);

		// Remove the operands which are induced by 1 other operand
		ImmutableSet<ISliceFilter> strippedAgainst1 = removeLaxerOrStricterGivenOne(removeLaxerElseStricter, toProcess);

		Collection<ISliceFilter> strippedAgainstAll =
				removeLaxerOrStricterGivenAll(removeLaxerElseStricter, strippedAgainst1);

		return AdhocCollectionHelpers.copyOfSets(toIgnore, strippedAgainstAll);
	}

	protected Map<Boolean, ImmutableSet<ISliceFilter>> partitionByPotentialInteraction(
			Set<? extends ISliceFilter> operands) {
		Set<Set<? extends ISliceFilter>> clusters = FilterHelpers.clusterFilters(operands);

		// Cluster with a single element can not interact with any other filter
		Map<Boolean, List<Set<? extends ISliceFilter>>> partitionedClusters =
				clusters.stream().collect(Collectors.partitioningBy(cluster -> cluster.size() == 1));

		return partitionedClusters.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getKey,
						e -> e.getValue().stream().flatMap(ee -> ee.stream()).collect(ImmutableSet.toImmutableSet())));
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

				// `a&b&e&(c|a&b)` -> `a&b`
				boolean orIsImplied = orMayBeDiscarded.getOperands()
						.stream()
						// `a&b&e` is stricter than `a&b` so `(c|a&b)` is matchAll
						.anyMatch(orOperand -> filterStripperFactory.makeFilterStripper(otherAsAnd)
								.isStricterThan(orOperand));

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

			// if removeLaxerElseStricter is true, this is the stricter operand
			ISliceFilter notFinalHarder = operandsAsList.getFirst();

			for (ISliceFilter candidate : operandsAsList) {
				if (candidate != notFinalHarder) {
					if (removeLaxerElseStricter && isStricterThan(candidate, notFinalHarder)) {
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
				isSofter = sf -> isStricterThan(sf, harder);
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
				if (isStricterThan(laxer, stricter)) {
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

	protected boolean isStricterThan(ISliceFilter where, ISliceFilter filter) {
		return filterStripperFactory.makeFilterStripper(where).isStricterThan(filter);
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

	/**
	 * 
	 * @param filters
	 * @return `splitAnd` input filters, excluding filters which would not interact with others
	 */
	protected ImmutableSet<? extends ISliceFilter> splitAnd(ImmutableSet<? extends ISliceFilter> filters) {
		Set<Set<? extends ISliceFilter>> clusters = FilterHelpers.clusterFilters(filters);

		return clusters.stream().flatMap(cluster -> {
			// TODO This switch on splitMatchers needs to be explained, or removed
			// SplitAnd to flatten imbricated `AND`, but keep matchers (e.g. `Not(In(...))`) as this split is
			// typically used to detect future interactions
			// filters which may interact are fully split.
			boolean splitMatchers = cluster.size() >= 2;

			return cluster.stream()
					.<ISliceFilter>mapMulti(
							(f, downstream) -> FilterHelpers.emitAndOperands(f, downstream, splitMatchers))
					// Skipping matchAll is useful on `.edit`
					.filter(f -> !f.isMatchAll());
		}).collect(ImmutableSet.toImmutableSet());

	}

	protected ISliceFilter negatePreferNotOrOverAndNot(ISliceFilter filter) {
		if (filter instanceof IOrFilter orFilter) {
			return preferNotOrOverAndNot(
					orFilter.getOperands()
							.stream()
							.map(this::negatePreferNotOrOverAndNot)
							.collect(ImmutableSet.toImmutableSet()),
					false);
		} else if (filter instanceof IAndFilter andFilter) {
			return NotFilter.simpleNot(
					preferNotOrOverAndNot(andFilter.getOperands().stream().collect(ImmutableSet.toImmutableSet()),
							true));
		} else {
			return filter.negate();
		}
	}

	/**
	 * BEWARE This method is quite dangerous/sensitive. Given a {@link ISliceFilter} is used as key in hashed structure,
	 * and {@link ISliceFilter} may be recombined through operations (e.g. split between `WHERE` and `FILTER` which are
	 * later `AND`-ed together). Hence, we need to have a single representation per equivalent boolean expression.
	 * 
	 * Using a cost-function is seductive, but it may lead to issues. Typically, if two equivalent representations has
	 * the same cost, we must also pick the same representation.
	 * 
	 * @param willBeNegated
	 *            if true, indicates the output will be wrapped in a `NOT`
	 * @param and
	 * @return
	 */
	// BEWARE This should not to any `.optimize` as it should receive an optimized expression, and choose between 2
	// equivalent representations.
	protected ISliceFilter preferNotOrOverAndNot(Set<? extends ISliceFilter> and, boolean willBeNegated) {
		if (and.size() >= 2) {
			// TODO Is this still useful since we introduced Kernel factorization?

			// `!(a==a1&b==b1)&!(a==a1&b==b2)&!(a==a1&b==b3)` can be turned into `a!=a1|b=out=(b1,b2,b3)`
			FilterUtility filterUtility = FilterUtility.builder().optimizer(this).build();
			ISliceFilter commonOr = filterUtility.commonOr(and);

			if (!ISliceFilter.MATCH_NONE.equals(commonOr)) {
				List<ISliceFilter> toAnd = and.stream()
						.map(f -> FilterHelpers.simplifyOrGivenContribution(filterStripperFactory, commonOr, f))
						.toList();

				ISliceFilter others = and(toAnd, willBeNegated);
				if (ISliceFilter.MATCH_NONE.equals(others)) {
					// Special branch as the other branch does `.combine` which skip optimizations
					// Happens on `(a|b==b1)&(a|b==b2)`, which is `a|b==b1&b==b2`, which is `a`
					return commonOr;
				} else {
					// Ensure this path also go through the `preferNotOrOverAndNot`
					// return FilterBuilder.or(commonOr, others).combine();
					ISliceFilter orToNegate = preferNotOrOverAndNot(
							ImmutableSet.of(NotFilter.simpleNot(commonOr), NotFilter.simpleNot(others)),
							!willBeNegated);
					return NotFilter.simpleNot(orToNegate);
				}
			}
		} else if (and.size() == 1 && AdhocCollectionHelpers.getFirst(and) instanceof ColumnFilter columnFilter) {
			// Skip optimization if already a trivial
			// BEWARE This may be faulty if the costFunction varies on IValueMatcher (i.e. come IColumnFilter may
			// deserve being negated)
			return columnFilter;
		}

		// Consider returning a `!(a|b)` instead of `!a&!b`
		ISliceFilter orCandidate =
				FilterBuilder.or(and.stream().map(this::negatePreferNotOrOverAndNot).toList()).combine();
		ISliceFilter notOrCandidate = orCandidate.negate();

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

		// Do not `.optimize` else we will get into a recursive loop
		// This may require `.combine` holds minimal optimizations (like basic flattening)
		return FilterBuilder.and(and).combine();
	}

	/**
	 * Groups the AND operands in {@code filters} by independent column clusters using union-find, then optimizes each
	 * cluster independently.
	 *
	 * <p>
	 * Two filters belong to the same cluster when their column sets (as returned by
	 * {@link FilterHelpers#getFilteredColumns}) are not disjoint. Clusters whose columns are fully disjoint cannot
	 * interact, so each can be passed to {@link #and} separately. The per-cluster results are then AND-ed with a
	 * structural (non-recursive) {@link FilterBuilder#and}.
	 *
	 * <p>
	 * Returns {@code null} when all operands belong to a single cluster (no independent split is possible), so the
	 * caller can proceed with the normal pipeline.
	 *
	 * @param filters
	 *            the flattened AND operands to partition
	 * @param willBeNegated
	 *            forwarded to each per-cluster {@link #and} call
	 * @return the optimized {@link ISliceFilter} for each cluster
	 */
	protected ImmutableSet<? extends ISliceFilter> optimizeByClusters(ImmutableSet<? extends ISliceFilter> filters,
			boolean willBeNegated) {
		if (filters.isEmpty()) {
			return filters;
		}

		Collection<Set<? extends ISliceFilter>> clusters = FilterHelpers.clusterFilters(filters);

		if (clusters.size() == 1) {
			// `.andOneCluster()` skips cache/recursive-loop
			return andOneCluster(AdhocCollectionHelpers.getFirst(clusters), willBeNegated);
		} else {
			// Optimize each cluster independently.
			ImmutableSet.Builder<ISliceFilter> results = ImmutableSet.builder();
			for (Set<? extends ISliceFilter> cluster : clusters) {
				// Each cluster relies on cache:
				ISliceFilter optimizedCluster = and(cluster, willBeNegated);
				if (optimizedCluster.isMatchNone()) {
					// break `AND` early on any `matchNone`
					return ImmutableSet.of(ISliceFilter.MATCH_NONE);
				}
				results.add(optimizedCluster);
			}

			return results.build();
		}
	}

	protected ISliceFilter notCachedNot(ISliceFilter filter, boolean willBeNegated) {
		if (filter instanceof IAndFilter andFilter) {
			ISliceFilter negated = and(andFilter.getOperands(), willBeNegated);

			return NotFilter.simpleNot(negated);
		} else if (filter instanceof IOrFilter orFilter) {
			// Plays optimizations given an `AND` of `NOT`s.
			// We may prefer `c!=c1&d==d2` over `!(c==c1|d!=d2)`
			return and(orFilter.getOperands().stream().map(ISliceFilter::negate).toList(), !willBeNegated);
			// ISliceFilter negated = or(orFilter.getOperands());

			// return NotFilter.simpleNot(negated);
		} else if (filter.isColumnFilter() && filter instanceof ColumnFilter columnFilter) {
			// Prefer `c!=c1` over `!(c==c1)`
			return columnFilter.toBuilder().matching(NotMatcher.not(columnFilter.getValueMatcher())).build();
		}
		return NotFilter.simpleNot(filter);
	}
}
