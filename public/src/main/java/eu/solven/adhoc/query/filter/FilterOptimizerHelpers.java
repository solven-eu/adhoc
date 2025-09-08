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
package eu.solven.adhoc.query.filter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.util.AdhocCollectionHelpers;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import lombok.experimental.UtilityClass;
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
@UtilityClass
public class FilterOptimizerHelpers {

	// `first, second, more` syntax to push providing at least 2 arguments
	public static ISliceFilter and(ISliceFilter first, ISliceFilter second, ISliceFilter... more) {
		if (more.length == 0 && first.equals(second)) {
			return first;
		}
		return and(Lists.asList(first, second, more));
	}

	public static ISliceFilter and(Collection<? extends ISliceFilter> filters) {
		return and(filters, false);
	}

	/**
	 * 
	 * @param filters
	 * @param willBeNegated
	 *            true if this expression will be negated (e.g. when being called by `OR`)
	 * @return
	 */
	protected static ISliceFilter and(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		// We need to start by flattening the input (e.g. `AND(AND(a=a1,b=b2)&a=a2)` to `AND(a=a1,b=b2,a=a2)`)
		ISliceFilter flatten = andNotOptimized(filters, willBeNegated);

		// TableQueryOptimizer.canInduce would typically do an `AND` over an `OR`
		// So we need to optimize `(c=c1) AND (c=c1 OR d=d1)`
		if (flatten instanceof IAndFilter andFilter) {
			flatten = optimizeAndOfOr(andFilter.getOperands());
		} else if (flatten instanceof INotFilter notFilter && notFilter.getNegated() instanceof IOrFilter orFilter) {
			// This is a AND optimized as a `Not(Or(...))`
			flatten = optimizeAndOfOr(orFilter.getOperands().stream().map(NotFilter::not).toList());
		}

		if (flatten instanceof IAndFilter andFilter) {
			// Then, we simplify given columnFilters (e.g. `a=a1&a=a2` to `matchNone`)
			Collection<? extends ISliceFilter> packedColumns = packColumnFilters(andFilter.getOperands());

			// Then simplify the AND (e.g. `AND(a=a1)` to `a=a1`)
			return andNotOptimized(packedColumns, willBeNegated);
		} else {
			return flatten;
		}
	}

	/**
	 * Dedicated to turn `(a==a1|b==b1)&(a==a1)` to `(a==a1)`.
	 * 
	 * TODO turn `!(a==a1&b==b1)&(a!=a1)` to `(a==a1)`.
	 * 
	 * @param operands
	 * @return
	 */
	// https://en.m.wikipedia.org/wiki/Logic_optimization
	// https://en.m.wikipedia.org/wiki/Quine%E2%80%93McCluskey_algorithm
	// https://en.m.wikipedia.org/wiki/Espresso_heuristic_logic_minimizer
	// BEWARE This algorithm is not smart at all, as it catches only a very limited number of cases.
	// One may contribute a finer implementation.
	private static ISliceFilter optimizeAndOfOr(Collection<ISliceFilter> operands) {
		// 1- Segment filters between OR-like filters and others
		// OR-like filters will be combined with a cartesian product, in the hope of rejecting many irrelevant
		// combinations. The others are combined as a single AND.

		// TODO Manage `Column(In(...))` but beware of cartesian products
		Map<Boolean, List<ISliceFilter>> orNotOr = operands.stream()
				.collect(Collectors.partitioningBy(o -> o instanceof IOrFilter
						|| o instanceof INotFilter notFilter && notFilter.getNegated() instanceof IAndFilter));

		List<ISliceFilter> orOperands = orNotOr.get(true);
		if (orOperands.isEmpty()) {
			// There is no OR : this is a plain AND.
			return new AndFilter(operands);
		}

		// Holds all AND operands which are not ORs.
		ISliceFilter simpleAnd = FilterBuilder.and(orNotOr.get(false)).optimize();

		// Register if at least one simplification occurred. If false, this is useful not to later call `Or.or`
		// which would lead to a cycle as `Or.or` is based on `And.and` which is based on current method.
		AtomicBoolean hasSimplified = new AtomicBoolean();

		if (simpleAnd.isMatchNone()) {
			return ISliceFilter.MATCH_NONE;
		}
		List<List<ISliceFilter>> orFilters =
				orOperands.stream().map(FilterOptimizerHelpers::getOrOperands).map(orFilter -> {
					List<ISliceFilter> operand = orFilter.stream()
							// Filter the combinations which are simplified into matchNone
							.filter(sf -> {
								ISliceFilter combinedOrOperand = AndFilter.and(simpleAnd, sf);

								boolean matchNone = combinedOrOperand.isMatchNone();
								if (matchNone) {
									// Reject this operand which is irrelevant
									hasSimplified.set(true);
								}

								return !matchNone;
							})
							.collect(Collectors.toCollection(ArrayList::new));

					// Simplify the OR before doing the cartesian product
					int sizeBefore = operand.size();
					removeStricterInOr(operand);
					if (operand.size() < sizeBefore) {
						hasSimplified.set(true);
					}

					return operand;
				}).toList();

		// This method prevents `Lists.cartesianProduct` to throw if the cartesianProduct is larger than
		// Integer.MAX_VALUE
		BigInteger cartesianProductSize = AdhocCollectionHelpers.cartesianProductSize(orFilters);

		// If the cartesian product is too large, it is unclear if we prefer to fail, or to skip the optimization
		// Skipping the optimization might lead to later issue, preventing recombination of CubeQueryStep
		if (cartesianProductSize.compareTo(BigInteger.valueOf(AdhocUnsafe.cartesianProductLimit)) > 0) {
			// throw new NotYetImplementedException("Faulty optimization on %s".formatted(operands));
			return new AndFilter(operands);
		}

		// Holds the set of flatten entries (e.g. given `(a|b)&(c|d)`, it holds `a&c`, `a&d`, `b&c` and `b&d`).
		// BEWARE Do we rely want to do a cartesianProduct if case of an `IN` with a large number of operands?
		List<List<ISliceFilter>> cartesianProduct = Lists.cartesianProduct(orFilters);

		Set<ISliceFilter> ors = cartesianProduct.stream()
				.map(FilterOptimizerHelpers::and)
				// Combine the simple AND (based on not OR operands) with the orEntry.
				// Keep the simple AND on the left, as they are common to all entries, hence easier to be read
				// if first
				// Some entries would be filtered out
				// e.g. given `(a|b)&(!a|c)`, the entry `a&!a` isMatchNone
				.filter(sf -> {
					ISliceFilter combinedOrOperand = AndFilter.and(simpleAnd, sf);

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
		if (hasSimplified.get()) {
			// At least one simplification occurred: it is relevant to build and optimize an OR over the leftover
			// operands

			// BEWARE This heuristic is very weak. We should have a clearer score to define which expressions is
			// better/simpler/faster. Generally speaking, we prefer AND over OR.
			// BEWARE We go into another batch of optimization for OR, which is safe as this is strictly simpler than
			// current input
			ISliceFilter orCandidate = FilterBuilder.and(simpleAnd, FilterBuilder.or(ors).optimize()).optimize();
			int costSimplifiedOr = costFunction(orCandidate);
			int costInputAnd = costFunction(operands);

			if (costSimplifiedOr < costInputAnd) {
				return orCandidate;
			}
		}

		// raw AND as we do not want to call optimizations recursively
		return FilterBuilder.and(operands).combine();
	}

	/**
	 * Given laxer operand imply stricter operands in OR, this will strip stricter operands.
	 * 
	 * @param orOperands
	 *            operands which are ORed.
	 */
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	private static void removeStricterInOr(Collection<? extends ISliceFilter> orOperands) {
		// Given a list of ORs, we reject stricter operands in profit of the laxer operands
		Map<ISliceFilter, ISliceFilter> inducedToInducer = new LinkedHashMap<>();

		orOperands.stream().forEach(inducer -> {
			orOperands.stream()
					// filter induced is stricter than inducer
					.filter(induced -> induced != inducer && FilterHelpers.isStricterThan(induced, inducer))
					.forEach(induced -> {
						// BEWARE an induced may have multiple inducer
						inducedToInducer.put(induced, inducer);
					});
		});

		inducedToInducer.forEach((induced, inducer) -> {
			// Remove entry one by one, else we fear we may remove an inducer
			// BEWARE Is it legit? I mean, should we just remove all inducers in all cases?
			// TODO There is a bug in case of hierarchy of inducer, and we remove the intermediate inducer: the
			// deeper inducer would then not removed
			if (orOperands.contains(inducer)) {
				orOperands.remove(induced);
			}
		});
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	private static Set<ISliceFilter> removeLaxerInAnd(Collection<? extends ISliceFilter> andOperands) {
		// Given a list of ANDs, we reject stricter operands in profit of the laxer operands
		Multimap<ISliceFilter, ISliceFilter> stricterToLaxer =
				MultimapBuilder.linkedHashKeys().arrayListValues().build();

		andOperands.stream().forEach(stricter -> {
			andOperands.stream()
					// filter induced is stricter than inducer
					.filter(laxer -> laxer != stricter && FilterHelpers.isStricterThan(stricter, laxer))
					.forEach(laxer -> {
						// BEWARE a laxer may have multiple stricter
						stricterToLaxer.put(stricter, laxer);
					});
		});

		Set<ISliceFilter> stripped = new LinkedHashSet<>(andOperands);

		stricterToLaxer.forEach((stricter, laxer) -> {
			// Remove entry one by one, else we fear we may remove an inducer
			// BEWARE Is it legit? I mean, should we just remove all inducers in all cases?
			// TODO There is a bug in case of hierarchy of inducer, and we remove the intermediate inducer: the
			// deeper inducer would then not removed
			if (stripped.contains(stricter)) {
				stripped.remove(laxer);
			}
		});

		return ImmutableSet.copyOf(stripped);
	}

	private static List<ISliceFilter> getOrOperands(ISliceFilter f) {
		if (f instanceof IOrFilter orFilter) {
			return ImmutableList.copyOf(orFilter.getOperands());
		} else {
			IAndFilter negatedAnd = (IAndFilter) ((INotFilter) f).getNegated();
			return ImmutableList.copyOf(negatedAnd.getOperands().stream().map(NotFilter::not).toList());
		}
	}

	/**
	 * Evaluate the cost of an `AND` given its operands
	 * 
	 * @param operands
	 * @return the cost of given operands, considered as being AND together.
	 */
	static int costFunction(Collection<? extends ISliceFilter> operands) {
		return operands.stream().mapToInt(FilterOptimizerHelpers::costFunction).sum();
	}

	/**
	 * Given a formula, the cost can be evaluated manually by counting the number of each operators. `AND` is free, `OR`
	 * cost `2`, `!` cost 3, any value matcher costs `1`.
	 * 
	 * @param f
	 * @return
	 */
	// factors are additive (hence not multiplicative) as we prefer a high `Not` (counting once) than multiple deep Not
	@SuppressWarnings("checkstyle:MagicNumber")
	static int costFunction(ISliceFilter f) {
		if (f instanceof IAndFilter andFilter) {
			return costFunction(andFilter.getOperands());
		} else if (f instanceof INotFilter notFilter) {
			// `Not` costs 3: we prefer one OR than one NOT
			return 2 + costFunction(notFilter.getNegated());
		} else if (f instanceof IColumnFilter columnFilter) {
			// `Not` costs 3: we prefer one OR than one NOT
			return costFunction(columnFilter.getValueMatcher());
		} else if (f instanceof IOrFilter orFilter) {
			return 2 + costFunction(orFilter.getOperands());
		} else {
			throw new NotYetImplementedException("Not managed: %s".formatted(f));
		}
	}

	static int costFunction(IValueMatcher m) {
		if (m instanceof NotMatcher notMatcher) {
			// `Not(c=c1)` will cost `3`
			return 2 + costFunction(notMatcher.getNegated());
		} else {
			return 1;
		}
	}

	// Like `and` but skipping the optimization. May be useful for debugging
	private static ISliceFilter andNotOptimized(Collection<? extends ISliceFilter> filters, boolean willBeNegated) {
		if (filters.stream().anyMatch(ISliceFilter::isMatchNone)) {
			return IAndFilter.MATCH_NONE;
		}

		// Skipping matchAll is useful on `.edit`
		Set<? extends ISliceFilter> notMatchAll = filters.stream().filter(f -> !f.isMatchAll()).flatMap(operand -> {
			if (operand instanceof IAndFilter operandIsAnd) {
				// AND of ANDs
				return operandIsAnd.getOperands().stream();
			} else if (operand instanceof INotFilter notFilter) {
				if (notFilter.getNegated() instanceof IOrFilter orFilter) {
					// NOT of ORs is AND of NOTs
					return orFilter.getOperands().stream().map(NotFilter::not);
					// } else if (notFilter.getNegated() instanceof IAndFilter andFilter) {
					// NOT of ANDs is AND of NOTs
					// return andFilter.getOperands().stream().map(NotFilter::not);
				}
				return Stream.of(operand);
			} else {
				return Stream.of(operand);
			}
		}).collect(ImmutableSet.toImmutableSet());

		if (notMatchAll.isEmpty()) {
			return IAndFilter.MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.iterator().next();
		}

		Set<? extends ISliceFilter> stripDedundancy = removeLaxerInAnd(notMatchAll);

		ISliceFilter orCandidate =
				OrFilter.builder().filters(stripDedundancy.stream().map(NotFilter::not).toList()).build();
		ISliceFilter notOrCandidate = NotFilter.builder().negated(orCandidate).build();
		int costOrCandidate;
		if (willBeNegated) {
			costOrCandidate = costFunction(orCandidate);
		} else {
			costOrCandidate = costFunction(notOrCandidate);
		}
		if (costOrCandidate < costFunction(stripDedundancy)) {
			return notOrCandidate;
		}

		return new AndFilter(stripDedundancy);
	}

	/**
	 * Helps packColumnFilters
	 * 
	 * @author Benoit Lacelle
	 */
	private static final class PackingColumns {
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

		private void registerPackedColumn(AtomicBoolean isMatchNone, IColumnFilter columnFilter) {
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
				} else {
					implicitFilters.add(columnFilter);
				}
			} else {
				implicitFilters.add(columnFilter);
			}
		}

		private void addOperandInSet(AtomicBoolean isMatchNone, Set<?> operands) {
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

		private void addOperandInSet(AtomicBoolean isMatchNone, Object operand) {
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

		public void collectPacks(AtomicBoolean isMatchNone,
				String column,
				Builder<ISliceFilter> packedFiltersBuilder,
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
					packedFiltersBuilder.add(ColumnFilter.isIn(column, allowedValues));
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
					packedFiltersBuilder.add(NotFilter.not(ColumnFilter.isIn(column, disallowedValues)));
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
	private static Collection<? extends ISliceFilter> packColumnFilters(Collection<? extends ISliceFilter> filters) {
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

				PackingColumns packingColumns = new PackingColumns();

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
				return Arrays.asList(IAndFilter.MATCH_NONE);
			} else {
				return packedFiltersBuilder
						// Add not managed at the end for readability, as they are generally more complex
						.addAll(notManaged)
						.build();
			}
		}
	}

}
