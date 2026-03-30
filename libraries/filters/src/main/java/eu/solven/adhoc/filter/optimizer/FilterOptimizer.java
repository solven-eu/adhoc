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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.IColumnFilter;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.NotFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.filter.stripper.IFilterStripper;
import eu.solven.adhoc.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.util.AdhocTime;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
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
	@NonNull
	final IOptimizerEventListener listener = new IOptimizerEventListener() {

	};

	@Default
	@NonNull
	final IFilterCostFunction costFunction = new StandardFilterCostFunction();

	@Default
	@Getter
	IFilterStripperFactory filterStripperFactory = AdhocFilterUnsafe.filterStripperFactory;

	@Default
	@NonNull
	Function<FilterOptimizer, KernelFactorizer> kernelFactorizerFactory = fo -> new KernelFactorizer(fo);

	@Override
	public ISliceFilter and(Set<? extends ISliceFilter> filters, boolean willBeNegated) {
		AndFilter filterForEvent = AndFilter.builder().ands(filters).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.unsafeClock);
		try {
			Optional<ISliceFilter> optTrivialOptimal = optAndTrivialOptimal(filters);
			if (optTrivialOptimal.isPresent()) {
				return optTrivialOptimal.get();
			} else {
				return notTrivialAnd(filters, willBeNegated);
			}
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	@Override
	public ISliceFilter or(Set<? extends ISliceFilter> filters, boolean willBeNegated) {
		OrFilter filterForEvent = OrFilter.builder().ors(filters).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.unsafeClock);
		try {
			if (filters.size() == 1) {
				// Prevent caching as OR what is a simpler AND.
				return and(filters, willBeNegated);
			} else {
				return notTrivialOr(filters, willBeNegated);
			}
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	/**
	 * For trivial {@link ISliceFilter}, we prefer to skip most of the computation. It will make debug easier, and will
	 * prevent the cache to be cluttered with trivial entries.
	 * 
	 * @param filters
	 * @return
	 */
	protected Optional<ISliceFilter> optAndTrivialOptimal(Collection<? extends ISliceFilter> filters) {
		if (filters.contains(ISliceFilter.MATCH_NONE)) {
			// Prevent caching trivial case
			return Optional.of(ISliceFilter.MATCH_NONE);
		} else if (filters.isEmpty()) {
			// Prevent caching trivial case
			return Optional.of(ISliceFilter.MATCH_ALL);
		} else if (filters.size() == 1) {
			ISliceFilter singleFilter = AdhocCollectionHelpers.getFirst(filters);

			if (singleFilter instanceof IColumnFilter columnFilter) {
				if (columnFilter.getValueMatcher() instanceof EqualsMatcher) {
					// `a==a1`
					return Optional.of(singleFilter);
				} else if (columnFilter.getValueMatcher() instanceof NotMatcher notMatcher
						&& notMatcher.getNegated() instanceof EqualsMatcher) {
					// `a!=a1`
					return Optional.of(singleFilter);
				}
			}
		}

		return Optional.empty();
	}

	@Override
	public ISliceFilter not(ISliceFilter filter, boolean willBeNegated) {
		NotFilter filterForEvent = NotFilter.builder().negated(filter).build();
		listener.onOptimize(filterForEvent);

		Instant start = Instant.now(AdhocTime.unsafeClock);
		try {
			return notTrivialNot(filter, willBeNegated);
		} finally {
			listener.onOptimizationDone(filterForEvent, AdhocTime.untilNow(start));
		}
	}

	protected ISliceFilter notTrivialAnd(Set<? extends ISliceFilter> filters, boolean willBeNegated) {
		Optional<ISliceFilter> optTrivialOptimal = optAndTrivialOptimal(filters);
		if (optTrivialOptimal.isPresent()) {
			return optTrivialOptimal.get();
		}

		// Very first: we cluster. It will help simplify many complex input (e.g. a large OR of AND, where AND generally
		// shares at least a few operands)
		Set<Set<? extends ISliceFilter>> clusters = FilterHelpers.clusterFilters(filters);

		// Each cluster can be processed independently, as they will be simple joined as AND in the and
		ImmutableSet<ISliceFilter> outputFilters = clusters.stream()
				.flatMap(cluster -> andOneCluster1(cluster, willBeNegated))
				.filter(f -> !ISliceFilter.MATCH_ALL.equals(f))
				.collect(ImmutableSet.toImmutableSet());

		if (outputFilters.contains(ISliceFilter.MATCH_NONE)) {
			return ISliceFilter.MATCH_NONE;
		} else if (outputFilters.isEmpty()) {
			return ISliceFilter.MATCH_ALL;
		} else if (outputFilters.size() == 1) {
			return AdhocCollectionHelpers.getFirst(outputFilters);
		} else {
			ImmutableSet<? extends ISliceFilter> preferredForm = preferNotOrOverAndNot(outputFilters, willBeNegated);
			return FilterBuilder.and(preferredForm).combine();
		}

	}

	protected Stream<? extends ISliceFilter> andOneCluster1(Set<? extends ISliceFilter> cluster,
			boolean willBeNegated) {
		// We need to start by flattening the input (e.g. `AND(AND(a=a1,b=b2)&a=a2)` to `AND(a=a1,b=b2,a=a2)`)
		ImmutableSet<? extends ISliceFilter> flatten = splitAnd(cluster);

		// Split by independent column clusters: filters whose columns do not overlap can be optimized
		// independently, then AND-ed together. This lets the 2-element commonOr logic in
		// preferNotOrOverAndNot handle subsets that share columns without being distracted by
		// unrelated filters.

		// Normalization refers to grouping columns together, and discarding irrelevant operands
		ImmutableSet<? extends ISliceFilter> stripRedundancyPre = normalizeOperands(flatten);
		if (stripRedundancyPre.isEmpty()) {
			// matchAll
			return Stream.empty();
		}

		ImmutableSet<? extends ISliceFilter> postKernelFactorization;
		if (stripRedundancyPre.size() >= 2) {
			// TableQueryOptimizer.canInduce would typically do an `AND` over an `OR`
			// So we need to optimize `(c=c1) AND (c=c1 OR d=d1)`
			KernelFactorizer kernelFactorizer = kernelFactorizerFactory.apply(this);
			postKernelFactorization = kernelFactorizer.optimizeAndOfOr(stripRedundancyPre, willBeNegated);

		} else {
			postKernelFactorization = stripRedundancyPre;
		}
		// Normalize again after the cartesianProduct
		// TODO Skip if cartesianProduct had no effect
		ImmutableSet<? extends ISliceFilter> clusterResults = optimizeOperands(postKernelFactorization, willBeNegated);

		if (clusterResults.contains(ISliceFilter.MATCH_NONE)) {
			return Stream.of(ISliceFilter.MATCH_NONE);
		} else if (clusterResults.isEmpty()) {
			return Stream.of(ISliceFilter.MATCH_ALL);
		}

		return clusterResults.stream();
	}

	protected ImmutableSet<? extends ISliceFilter> normalizeOperands(Set<? extends ISliceFilter> cluster) {
		if (cluster.size() <= 1) {
			return ImmutableSet.copyOf(cluster);
		}

		ImmutableSet<? extends ISliceFilter> simplifiedGivenOthers = simplifyEachGivenOthers(cluster);

		// Then, we simplify given columnFilters (e.g. `a=a1&a=a2` to `matchNone`)
		ImmutableSet<? extends ISliceFilter> packedColumns =
				makeColumnPacker().packColumnFilters(splitAnd(simplifiedGivenOthers));

		if (packedColumns.contains(ISliceFilter.MATCH_NONE)) {
			return ImmutableSet.of(ISliceFilter.MATCH_NONE);
		}

		// given `a&b&c`, remove `c` if it is laxer than `a`.
		return removeLaxerInAnd(packedColumns);
	}

	protected ColumnPacker makeColumnPacker() {
		return new ColumnPacker(this);
	}

	// BEWARE This may lead to 2 operands simplify the other, leading to some operand being lost
	// given `a==a1&(a!=a1|b==b2)`, gives `a==a1&b==b2`
	protected ImmutableSet<? extends ISliceFilter> simplifyEachGivenOthers(Set<? extends ISliceFilter> andOperands) {
		if (andOperands.size() <= 1) {
			return ImmutableSet.copyOf(andOperands);
		}

		// The input as a List of `AND`. This list will be mutated in place
		List<ISliceFilter> asAnds = new ArrayList<>(splitAnd(andOperands));

		// BEWARE This is giving importance to the order, while ordering is not enforced
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

			if (ISliceFilter.MATCH_NONE.equals(simpler)) {
				return ImmutableSet.of(ISliceFilter.MATCH_NONE);
			}

			// `simpler` is simpler or same to the original expression: let's register it without trying it to compare
			// with the original expression.
			asAnds.set(i, simpler);
		}

		return ImmutableSet.copyOf(asAnds);
		// return ImmutableSet.copyOf(andOperands);
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
			return or(orFilter.getOperands(), willBeNegated);
		} else if (f instanceof INotFilter notFilter) {
			return not(notFilter.getNegated(), willBeNegated);
		} else {
			// BEWARE Should we have optimizations for IColumnFilter/IValueMatchers?
			return f;
		}
	}

	// https://en.wikipedia.org/wiki/De_Morgan%27s_laws
	protected ISliceFilter notTrivialOr(Set<? extends ISliceFilter> filters, boolean willBeNegated) {
		// Prepare without optimization
		Set<ISliceFilter> negated = filters.stream().map(ISliceFilter::negate).collect(ImmutableSet.toImmutableSet());

		// OR relies on AND optimizations
		ISliceFilter negatedOptimized = and(negated, !willBeNegated);

		// Unroll without optimization
		return NotFilter.simpleNot(negatedOptimized);
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
	 * @param filters
	 * @return `splitAnd` input filters, excluding filters which would not interact with others
	 */
	protected ImmutableSet<? extends ISliceFilter> splitAnd(Set<? extends ISliceFilter> filters) {
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

	/**
	 * This method is useful as it is unclear how `AndFilter` and `OrFilter` will return to `.negate`. In this case, we
	 * do not want a `!(...)` but explicitly switching `& <> |`.
	 * 
	 * @param filter
	 * @return
	 * @see eu.solven.adhoc.filter.NotFilter.simpleNot(ISliceFilter)
	 */
	protected ISliceFilter negatePreferNotOrOverAndNot(ISliceFilter filter) {
		if (filter instanceof IOrFilter orFilter) {
			ImmutableSet<ISliceFilter> operands = orFilter.getOperands()
					.stream()
					// Do NOT call `negatePreferNotOrOverAndNot` recursively
					.map(ISliceFilter::negate)
					.collect(ImmutableSet.toImmutableSet());
			return FilterBuilder.and(operands).combine();
		} else if (filter instanceof IAndFilter andFilter) {
			return NotFilter.simpleNot(andFilter);
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
	protected ImmutableSet<? extends ISliceFilter> preferNotOrOverAndNot(ImmutableSet<? extends ISliceFilter> and,
			boolean willBeNegated) {
		if (and.size() == 1 && AdhocCollectionHelpers.getFirst(and) instanceof ColumnFilter columnFilter) {
			// Skip optimization if already a trivial
			// BEWARE This may be faulty if the costFunction varies on IValueMatcher (i.e. come IColumnFilter may
			// deserve being negated)
			return ImmutableSet.of(columnFilter);
		}

		// Consider returning a `!(a|b)` instead of `!a&!b`
		ISliceFilter orCandidate =
				FilterBuilder.or(and.stream().map(this::negatePreferNotOrOverAndNot).toList()).combine();
		ISliceFilter notOrCandidate = orCandidate.negate();

		long costOrCandidate;
		long costAndCandidate;
		if (willBeNegated) {
			costOrCandidate = costFunction.cost(orCandidate);
			costAndCandidate = costFunction.cost(FilterBuilder.and(and).combine().negate());
		} else {
			costOrCandidate = costFunction.cost(notOrCandidate);
			costAndCandidate = costFunction.cost(and);
		}
		// BEWARE If same cost, we prefer `AND`
		if (costOrCandidate < costAndCandidate) {
			return ImmutableSet.of(notOrCandidate);
		}

		// Do not `.optimize` else we will get into a recursive loop
		// This may require `.combine` holds minimal optimizations (like basic flattening)
		return and;
	}

	protected ISliceFilter notTrivialNot(ISliceFilter filter, boolean willBeNegated) {
		if (filter instanceof IAndFilter andFilter) {
			ISliceFilter negated = and(andFilter.getOperands(), !willBeNegated);

			return NotFilter.simpleNot(negated);
		} else if (filter instanceof IOrFilter orFilter) {
			// Plays optimizations given an `AND` of `NOT`s.
			// We may prefer `c!=c1&d==d2` over `!(c==c1|d!=d2)`
			ImmutableSet<ISliceFilter> negatedAndOperands =
					orFilter.getOperands().stream().map(ISliceFilter::negate).collect(ImmutableSet.toImmutableSet());
			return and(negatedAndOperands, !willBeNegated);
		} else if (filter.isColumnFilter() && filter instanceof ColumnFilter columnFilter) {
			// Prefer `c!=c1` over `!(c==c1)`
			return columnFilter.toBuilder().matching(NotMatcher.not(columnFilter.getValueMatcher())).build();
		}
		return NotFilter.simpleNot(filter);
	}
}
