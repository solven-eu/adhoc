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
package eu.solven.adhoc.table.sql;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.True;
import org.jooq.impl.DSL;

import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.IColumnFilter;
import eu.solven.adhoc.filter.IHasOperands;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.filter.value.AndMatcher;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.filter.value.OrMatcher;
import eu.solven.adhoc.filter.value.StringMatcher;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory.ConditionWithFilter;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard implementation of {@link ISliceToJooqCondition}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class SliceToJooqCondition implements ISliceToJooqCondition {

	@NonNull
	final Function<String, Name> toName;

	/**
	 * Query-scoped optimizer used for all {@link FilterBuilder#optimize(IFilterOptimizer)} calls inside this instance.
	 * Defaults to {@link AdhocFilterUnsafe#filterOptimizer} so that callers that do not supply an optimizer get
	 * identical behaviour to before this field was introduced.
	 */
	@NonNull
	@Default
	IFilterOptimizer filterOptimizer = AdhocFilterUnsafe.filterOptimizer;

	protected ConditionWithFilter toCondition(ISliceFilter filter) {
		return toCondition(filter, false);
	}

	/**
	 * 
	 * @param filter
	 * @param hasParentNot
	 *            indicates if this expression is wrapped into a `NOT` which requires specific `NULL` management (e.g.
	 *            `NOT(c = 'v')` is false if c is `NULL`).
	 * @return
	 */
	protected ConditionWithFilter toCondition(ISliceFilter filter, boolean hasParentNot) {
		if (filter.isMatchAll()) {
			return buildMatchAllCondition();
		} else if (filter.isMatchNone()) {
			return buildMatchNoneCondition();
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			return handleColumnFilter(columnFilter, hasParentNot);
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return handleNotFilter(notFilter);
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return handleAndFilter(andFilter, hasParentNot);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return handleOrFilter(orFilter, hasParentNot);
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	private ConditionWithFilter buildMatchAllCondition() {
		return ConditionWithFilter.builder().condition(DSL.trueCondition()).build();
	}

	private ConditionWithFilter buildMatchNoneCondition() {
		return ConditionWithFilter.builder().condition(DSL.falseCondition()).build();
	}

	private ConditionWithFilter handleColumnFilter(IColumnFilter columnFilter, boolean hasParentNot) {
		Optional<Condition> optColumnFilterAsCondition = toCondition(columnFilter, hasParentNot);
		if (optColumnFilterAsCondition.isEmpty()) {
			log.debug("{} will be applied manually", columnFilter);
			return ConditionWithFilter.builder().leftover(columnFilter).build();
		}
		return ConditionWithFilter.builder().condition(optColumnFilterAsCondition.get()).build();
	}

	private ConditionWithFilter handleNotFilter(INotFilter notFilter) {
		ConditionWithFilter negated = toCondition(notFilter.getNegated(), true);

		boolean oneIsMatchAll = false;

		ISliceFilter negatedPostFilter;
		if (negated.getLeftover().isMatchAll()) {
			negatedPostFilter = ISliceFilter.MATCH_ALL;
			oneIsMatchAll = true;
		} else {
			negatedPostFilter = negated.getLeftover().negate();
		}

		Condition negatedCondition;
		if (negated.getCondition() instanceof True) {
			negatedCondition = DSL.trueCondition();
			oneIsMatchAll = true;
		} else {
			negatedCondition = negated.getCondition().not();
		}

		if (!oneIsMatchAll) {
			throw new NotYetImplementedException("Converting `%s` to SQL".formatted(notFilter));
		}

		return ConditionWithFilter.builder().leftover(negatedPostFilter).condition(negatedCondition).build();
	}

	private ConditionWithFilter handleAndFilter(IAndFilter andFilter, boolean hasParentNot) {
		Set<? extends ISliceFilter> operands = andFilter.getOperands();
		List<ConditionWithFilter> conditions = operands.stream().map(c -> toCondition(c, hasParentNot)).toList();

		List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();
		List<ISliceFilter> leftoversConditions = conditions.stream().map(ConditionWithFilter::getLeftover).toList();

		return and(sqlConditions, leftoversConditions);
	}

	private ConditionWithFilter handleOrFilter(IOrFilter orFilter, boolean hasParentNot) {
		Set<ISliceFilter> operands = orFilter.getOperands();

		List<ConditionWithFilter> conditions = operands.stream().map(c -> toCondition(c, hasParentNot)).toList();

		boolean anyPostFilter =
				conditions.stream().map(ConditionWithFilter::getLeftover).anyMatch(f -> !f.isMatchAll());

		if (anyPostFilter) {
			log.debug("A postFilter with OR (`{}`) leads to no table filtering", orFilter);
			return ConditionWithFilter.builder().condition(DSL.trueCondition()).leftover(orFilter).build();
		}

		List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();

		return ConditionWithFilter.builder().condition(DSL.or(sqlConditions)).leftover(ISliceFilter.MATCH_ALL).build();
	}

	@Override
	public ConditionWithFilter and(Collection<Condition> sqlConditions, Collection<ISliceFilter> leftoversConditions) {
		return ConditionWithFilter.builder()
				.condition(andSql(sqlConditions))
				.leftover(FilterBuilder.and(leftoversConditions).optimize(filterOptimizer))
				.build();
	}

	protected Condition andSql(Collection<Condition> sqlConditions) {
		List<Condition> notTrueConditions = sqlConditions.stream()
				// Typically happens on `COUNT(*)`
				.filter(c -> !(c instanceof True))
				.toList();

		if (notTrueConditions.isEmpty()) {
			return DSL.trueCondition();
		}

		return DSL.and(notTrueConditions);
	}

	/**
	 *
	 * @param columnFilter
	 * @return if empty, it means the {@link IColumnFilter} can not be transcoded into a {@link Condition}.
	 */
	protected Optional<Condition> toCondition(IColumnFilter columnFilter) {
		return toCondition(columnFilter, false);
	}

	protected Optional<Condition> toCondition(IColumnFilter columnFilter, boolean hasParentNot) {
		IValueMatcher valueMatcher = columnFilter.getValueMatcher();
		String column = columnFilter.getColumn();

		final Field<Object> field = DSL.field(toName.apply(column));

		Condition condition;
		switch (valueMatcher) {
		case NullMatcher unused -> condition = DSL.condition(field.isNull());
		case InMatcher inMatcher -> {
			Set<?> operands = inMatcher.getOperands();

			if (operands.stream().anyMatch(o -> o instanceof IValueMatcher)) {
				// Please fill a ticket, various such cases could be handled
				throw new UnsupportedOperationException("There is a IValueMatcher amongst " + operands);
			}

			condition = wrap(hasParentNot, field, field.in(operands));
		}
		case EqualsMatcher equalsMatcher -> condition = wrap(hasParentNot, field, field.eq(equalsMatcher.getOperand()));
		case LikeMatcher likeMatcher -> condition = wrap(hasParentNot, field, field.like(likeMatcher.getPattern()));
		case StringMatcher stringMatcher ->
			condition = wrap(hasParentNot, field, field.cast(String.class).eq(stringMatcher.getString()));

		case ComparingMatcher comparingMatcher -> {
			Object operand = comparingMatcher.getOperand();

			Condition jooqCondition;
			if (comparingMatcher.isGreaterThan()) {
				if (comparingMatcher.isMatchIfEqual()) {
					jooqCondition = field.greaterOrEqual(operand);
				} else {
					jooqCondition = field.greaterThan(operand);
				}
			} else {
				if (comparingMatcher.isMatchIfEqual()) {
					jooqCondition = field.lessOrEqual(operand);
				} else {
					jooqCondition = field.lessThan(operand);
				}
			}
			condition = wrap(hasParentNot, field, jooqCondition);
		}

		case AndMatcher andMatcher -> {
			List<Optional<Condition>> optConditions = toConditions(column, andMatcher, hasParentNot);

			if (optConditions.stream().anyMatch(Optional::isEmpty)) {
				return Optional.empty();
			}

			condition = DSL.and(optConditions.stream().map(Optional::get).toList());
		}
		case OrMatcher orMatcher -> {
			List<Optional<Condition>> optConditions = toConditions(column, orMatcher, hasParentNot);

			if (optConditions.stream().anyMatch(Optional::isEmpty)) {
				return Optional.empty();
			}

			condition = DSL.or(optConditions.stream().map(Optional::get).toList());
		}
		case NotMatcher notMatcher -> {
			Optional<Condition> optConditions =
					toCondition(ColumnFilter.builder().column(column).valueMatcher(notMatcher.getNegated()).build(),
							true);

			if (optConditions.isEmpty()) {
				return Optional.empty();
			}

			condition = DSL.not(optConditions.get());
		}
		default -> condition = onCustomCondition(column, valueMatcher, hasParentNot);
		}

		return Optional.ofNullable(condition);
	}

	protected Condition wrap(boolean hasParentNot, Field<Object> field, Condition condition) {
		if (hasParentNot) {
			// https://www.w3schools.com/sql/sql_not.asp
			// https://dba.stackexchange.com/questions/333948/not-equal-to-operator-is-not-returning-null-values-in-sql-server
			// https://stackoverflow.com/questions/5658457/not-equal-operator-on-null

			// Typically `not(c = 'v')` returns false if `c IS NULL` while `not(c IS NOT NULL AND c = 'v')` returns true
			return DSL.and(field.isNotNull(), condition);
		} else {
			return DSL.condition(condition);
		}
	}

	protected List<Optional<Condition>> toConditions(String column,
			IHasOperands<IValueMatcher> hasOperands,
			boolean hasParentNot) {
		return hasOperands.getOperands().stream().map(subValueMatcher -> {
			ColumnFilter subFilter = ColumnFilter.builder().column(column).valueMatcher(subValueMatcher).build();
			return toCondition(subFilter, hasParentNot);
		}).toList();
	}

	/**
	 * 
	 * @param column
	 * @param valueMatcher
	 * @param hasParentNot
	 * @return By default, we return null so that this {@link IValueMatcher} is managed as post-filtering by Adhoc, over
	 *         the {@link ITableWrapper} result.
	 */
	protected Condition onCustomCondition(String column, IValueMatcher valueMatcher, boolean hasParentNot) {
		// throw new UnsupportedOperationException(
		// "Not handled: %s matches %s".formatted(column, PepperLogHelper.getObjectAndClass(valueMatcher)));
		return null;
	}

	@Override
	public ConditionWithFilter toConditionSplitLeftover(ISliceFilter filter) {

		// Split `AND` to enable `preFilter AND postFilter`
		// This will also cover `NOT(OR(...))`
		Set<ISliceFilter> ands = FilterHelpers.splitAnd(filter);

		// Partition conditions which can be translated into SQL or not.
		// BEWARE It will lead to translating twice to SQL
		Map<Boolean, List<ISliceFilter>> conditionAndFilters =
				ands.stream().collect(Collectors.partitioningBy(f -> toCondition(f).getLeftover().isMatchAll()));

		ISliceFilter withoutPostFilter = FilterBuilder.and(conditionAndFilters.get(true)).optimize(filterOptimizer);
		ISliceFilter withPostFilter = FilterBuilder.and(conditionAndFilters.get(false)).optimize(filterOptimizer);

		if (ISliceFilter.MATCH_ALL.equals(withPostFilter)) {
			// There is no customCondition: restore the original condition as it may have be changed by the
			// `partitioninBy` and the `optimize`
			withoutPostFilter = filter;
		}

		ConditionWithFilter conditionWithout = toCondition(withoutPostFilter);
		if (!ISliceFilter.MATCH_ALL.equals(conditionWithout.getLeftover())) {
			throw new IllegalStateException("Expected no postFilter from %s".formatted(withoutPostFilter));
		}

		return ConditionWithFilter.builder()
				.condition(conditionWithout.getCondition())
				.leftover(withPostFilter)
				.build();
	}

}
