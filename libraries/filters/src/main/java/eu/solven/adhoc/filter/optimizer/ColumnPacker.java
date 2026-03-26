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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.IColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
@Builder
public class ColumnPacker {

	final IFilterOptimizer optimizer;

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

				PackingColumns packingColumns = new PackingColumns(this.optimizer);

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
}
