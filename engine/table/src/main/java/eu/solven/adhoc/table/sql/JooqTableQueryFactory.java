/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jooq.AggregateFunction;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.Select;
import org.jooq.SelectConnectByStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SortField;
import org.jooq.TableLike;
import org.jooq.True;
import org.jooq.impl.DSL;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.dataframe.row.AggregatedRecordFields;
import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.column.IAdhocColumn;
import eu.solven.adhoc.model.column.ReferencedColumn;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.groupby.IHasSqlExpression;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link ITableAliaser} relies on a {@link AliasingContext}
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
// Lombok @SuperBuilder synthesises a generic JooqTableQueryFactoryBuilder<C, B>; its fields are populated via
// chained setters and NullAway can't see that init pattern.
@SuppressWarnings({ "PMD.GodClass", "PMD.CouplingBetweenObjects", "NullAway.Init" })
public class JooqTableQueryFactory implements IJooqTableQueryFactory {

	@NonNull
	@Builder.Default
	final IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	/**
	 * Optional per-query table provider. When set, {@link #prepareSliceQuery(TableQueryV4)} substitutes the
	 * {@link #table} field with {@link IJooqTableSupplier#tableFor(TableQueryV4)}. When {@code null}, the constant
	 * {@link #table} is always used (current behaviour).
	 */
	@NonNull
	final IJooqTableSupplier tableSupplier;

	@NonNull
	final DSLContext dslContext;

	@Getter(AccessLevel.PACKAGE)
	final JooqTableCapabilities capabilities;

	@NonNull
	@Builder.Default
	final SliceToJooqConditionFactory sliceToCondition = new SliceToJooqConditionFactory();

	/**
	 * Query-scoped optimizer forwarded to every {@link SliceToJooqCondition} instance produced by
	 * {@link #makeToCondition()}. Defaults to {@link AdhocFilterUnsafe#filterOptimizer}.
	 */
	@NonNull
	@Builder.Default
	IFilterOptimizer filterOptimizer = AdhocFilterUnsafe.filterOptimizer;

	@NonNull
	@Default
	final IQueryPartitionor queryPartitionor = IQueryPartitionor.SINGLE_PARTITION;

	/**
	 * Manually-declared inner builder class. Lombok's {@link lombok.experimental.SuperBuilder} merges the
	 * auto-generated setters and fields with the members declared here; the only member we declare is the migration
	 * helper {@link #table(TableLike)}.
	 */
	@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
	public abstract static class JooqTableQueryFactoryBuilder<C extends JooqTableQueryFactory,
			B extends JooqTableQueryFactoryBuilder<C, B>> {
		/**
		 * Migration helper: accepts a constant {@link TableLike} and wires it as
		 * {@code tableSupplier(IJooqTableSupplier.constant(table))}. Prefer {@link #tableSupplier(IJooqTableSupplier)}
		 * directly when the {@code FROM} clause must vary per query.
		 *
		 * @param table
		 *            the constant {@link TableLike} to place in the {@code FROM} clause
		 * @return this builder, for chaining
		 */
		public B table(TableLike<?> table) {
			return this.tableSupplier(IJooqTableSupplier.constant(table));
		}
	}

	// BEWARE This is Delombokized. It customizes the case `capabilities == null`
	protected JooqTableQueryFactory(JooqTableQueryFactoryBuilder<?, ?> b) {
		if (b.operatorFactory$set) {
			this.operatorFactory = b.operatorFactory$value;
		} else {
			this.operatorFactory = $default$operatorFactory();
		}
		this.tableSupplier = b.tableSupplier;
		this.dslContext = b.dslContext;

		if (b.capabilities == null) {
			this.capabilities = JooqTableCapabilities.from(dslContext.dialect());
		} else {
			this.capabilities = b.capabilities;
		}
		if (b.sliceToCondition$set) {
			this.sliceToCondition = b.sliceToCondition$value;
		} else {
			this.sliceToCondition = $default$sliceToCondition();
		}
		if (b.queryPartitionor$set) {
			this.queryPartitionor = b.queryPartitionor$value;
		} else {
			this.queryPartitionor = $default$queryPartitionor();
		}
		if (b.filterOptimizer$set) {
			this.filterOptimizer = b.filterOptimizer$value;
		} else {
			this.filterOptimizer = $default$filterOptimizer();
		}
	}

	/**
	 * Holds a Set of SQL {@link Condition}s, given an {@link ISliceFilter}. Some filters may not be convertible into
	 * SQL. In such case, we ensure the columns are in the groupBy for manual filtering.
	 * 
	 * Both conditions can be considered as being ANDed together.
	 */
	@Value
	@Builder
	public static class ConditionWithFilter {
		// SQL conditions, translated from an IAdhocFilter
		@NonNull
		@Builder.Default
		Condition condition = DSL.trueCondition();
		// Holds the filter of the conditions which were not translated into SQL
		@NonNull
		@Builder.Default
		ISliceFilter nonPushdown = ISliceFilter.MATCH_ALL;
	}

	@Deprecated
	public QueryWithLeftover prepareSliceQuery(TableQueryV2 tableQuery) {
		return prepareSliceQuery(TableQueryV3.edit(tableQuery).build());
	}

	@Override
	public QueryWithLeftover prepareSliceQuery(TableQueryV4 tableQuery) {
		TableLike<?> fromTable = resolveTable(tableQuery);

		// Perfect V4: every groupBy shares the same FA set — one GROUPING-SET SQL with no wasteful cartesian.
		if (tableQuery.isPerfectV3()) {
			return prepareSliceQuery(tableQuery.toV3(), fromTable);
		}

		// Non-perfect V4: emit a SQL UNION ALL across the streamV3() branches so the DB only computes the
		// (groupBy, aggregator) pairs each branch actually requires. Replaces the prior asCoveringV3() shape,
		// which silently inflated to the full cartesian product.
		return prepareUnionAllSliceQuery(tableQuery, fromTable);
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected String percent(long numerator, long denominator) {
		return "%.1f%%".formatted(100.0 * numerator / denominator);
	}

	/**
	 * Resolves the {@link TableLike} to use in the {@code FROM} clause for a given query. Defaults to the constant
	 * {@link #table} unless an {@link IJooqTableSupplier} was wired via the builder, in which case it is consulted.
	 * Subclasses may override to plug in their own per-query logic.
	 */
	protected TableLike<?> resolveTable(TableQueryV4 tableQuery) {
		return tableSupplier.tableFor(tableQuery);
	}

	protected QueryWithLeftover prepareSliceQuery(TableQueryV3 tableQuery) {
		return prepareSliceQuery(tableQuery, resolveTable(TableQueryV4.edit(tableQuery).build()));
	}

	/**
	 * SLICES variant: GROUP BY + aggregate functions. Wraps each aggregator in its SQL aggregation function (e.g.
	 * {@code SUM(col) FILTER (WHERE ...)}) and applies a {@code GROUP BY} on the requested columns. One row per
	 * distinct slice.
	 */
	protected QueryWithLeftover prepareSliceQuery(TableQueryV3 tableQuery, TableLike<?> fromTable) {
		return prepareQuery(tableQuery, fromTable, SqlRenderMode.SLICES);
	}

	/**
	 * ROWS variant: no GROUP BY, no aggregate function. Each per-aggregator FILTER becomes a
	 * {@code CASE WHEN <filter> THEN <column> END AS <alias>} (the column is null when the FILTER does not match), so
	 * each surviving DB row produces one record. This is the foundation of
	 * {@link eu.solven.adhoc.options.StandardQueryOptions#DRILLTHROUGH}.
	 *
	 * @param tableQuery
	 *            the merged DRILLTHROUGH query.
	 * @return the {@link QueryWithLeftover} carrying the raw-rows SQL.
	 */
	@Override
	public QueryWithLeftover prepareRowsQuery(TableQueryV3 tableQuery) {
		return prepareQuery(tableQuery, resolveTable(TableQueryV4.edit(tableQuery).build()), SqlRenderMode.ROWS);
	}

	/**
	 * SQL rendering mode, capturing the only two axes by which {@link #prepareSliceQuery} and {@link #prepareRowsQuery}
	 * differ:
	 * <ul>
	 * <li>{@link #SLICES}: SELECT wraps each aggregator in its SQL aggregation function and a {@code GROUP BY} clause
	 * is emitted over the requested groupBy columns.</li>
	 * <li>{@link #ROWS}: SELECT emits a {@code CASE WHEN <filter> THEN <column>} per FA (no aggregation function) and
	 * no {@code GROUP BY} is emitted, so each surviving DB row produces one record.</li>
	 * </ul>
	 * Everything else (WHERE, FROM, leftover splitting, ORDER BY, partitioning, QueryWithLeftover assembly) is shared.
	 */
	protected enum SqlRenderMode {
		SLICES, ROWS
	}

	/**
	 * Shared scaffold for both {@link #prepareSliceQuery} and {@link #prepareRowsQuery}. The two methods only differ on
	 * how they render the SELECT clause (aggregate-functions-with-FILTER vs CASE-WHEN) and whether they emit a
	 * {@code GROUP BY} — both axes captured by {@link SqlRenderMode}.
	 */
	protected QueryWithLeftover prepareQuery(TableQueryV3 tableQuery, TableLike<?> fromTable, SqlRenderMode mode) {
		ISliceToJooqCondition toCondition = makeToCondition();

		ConditionWithFilter conditionAndNonPushdown = toConditions(toCondition, tableQuery);

		// Leftover in FILTER clause — common to both modes: any FA whose FILTER cannot be transcoded fully into
		// SQL records its leftover here, and the JooqTableWrapper applies the leftover post-fetch.
		Map<String, ISliceFilter> aggregateToNonPushdown = new LinkedHashMap<>();
		tableQuery.getAggregators().forEach(filtered -> {
			ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitNonPushdown(filtered.getFilter());
			ISliceFilter nonPushdown = conditionWithFilter.getNonPushdown();
			if (!nonPushdown.isMatchAll()) {
				aggregateToNonPushdown.put(filtered.getAlias(), nonPushdown);
			}
		});

		ImmutableSet<ISliceFilter> nonPushdowns = ImmutableSet.<ISliceFilter>builder()
				.add(conditionAndNonPushdown.getNonPushdown())
				.addAll(aggregateToNonPushdown.values())
				.build();
		AggregatedRecordFields fields = selectedColumns(tableQuery, nonPushdowns);

		// `SELECT ...` — the FIRST mode-specific axis.
		Collection<SelectFieldOrAsterisk> selectedFields = switch (mode) {
		case SLICES -> selectedSliceFields(toCondition, tableQuery, fields);
		case ROWS -> selectedRowsFields(toCondition, tableQuery, fields);
		};

		// `FROM ...`
		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(fromTable);

		// `WHERE ...`
		SelectConnectByStep<Record> selectFromWhere;
		if (conditionAndNonPushdown.getCondition() instanceof True) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(conditionAndNonPushdown.getCondition());
		}

		// `GROUP BY ...` — the SECOND mode-specific axis. ROWS emits no GROUP BY at all.
		ResultQuery<Record> beforeOrder = switch (mode) {
		case SLICES -> selectFromWhere
				.groupBy(makeGroupingFields(tableQuery, conditionAndNonPushdown.getNonPushdown()));
		case ROWS -> selectFromWhere;
		};

		// `ORDER BY ...` / `LIMIT ...`
		ResultQuery<Record> resultQuery;
		if (tableQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(tableQuery);
			resultQuery = applyOrderAndLimit(beforeOrder, optOrderFields, tableQuery.getTopClause().getLimit());
		} else {
			resultQuery = beforeOrder;
		}

		return QueryWithLeftover.builder()
				.queries(partitionQuery(resultQuery))
				.nonPushdown(conditionAndNonPushdown.getNonPushdown())
				.aggregatorToNonPushdowns(aggregateToNonPushdown)
				.fields(fields)
				.build();
	}

	/**
	 * Build a single SQL UNION ALL combining one branch per distinct aggregator set across the {@link TableQueryV4}'s
	 * groupBys (as exposed by {@link TableQueryV4#streamV3()}). Each branch carries only the (groupBy, aggregator)
	 * pairs it actually needs; every branch's SELECT projects the unified column set directly, with {@code NULL} for
	 * slots a branch does not carry and a constant grouping indicator ({@code 0} for always-grouped, {@code 1} for
	 * rolled-up) for columns the branch does not have a real {@code GROUPING(col)} for.
	 *
	 * <p>
	 * Only used when {@link TableQueryV4#isPerfectV3()} is false — when it is true, {@link #prepareSliceQuery} routes
	 * directly through a single GROUPING-SET query via {@link TableQueryV4#toV3()}.
	 */
	protected QueryWithLeftover prepareUnionAllSliceQuery(TableQueryV4 tableQuery, TableLike<?> fromTable) {
		List<TableQueryV3> branchV3s = tableQuery.streamV3().toList();
		if (branchV3s.isEmpty()) {
			throw new IllegalStateException("Expected at least one streamV3 branch: %s".formatted(tableQuery));
		}

		// First pass per branch: condition + leftover + branch-natural fields. Captured up-front because we need
		// the union of all branches' fields to build each branch's unified SELECT in the second pass.
		List<BranchContext> branches = branchV3s.stream().map(this::prepareBranchContext).toList();

		AggregatedRecordFields unifiedFields = unifyFields(branches.stream().map(BranchContext::fields).toList());

		List<Select<Record>> branchSelects =
				branches.stream().map(b -> buildUnionBranchSelect(b, unifiedFields, fromTable)).toList();
		Select<Record> union = branchSelects.get(0);
		for (int i = 1; i < branchSelects.size(); i++) {
			union = union.unionAll(branchSelects.get(i));
		}

		// `ORDER BY ...` / `LIMIT ...` on the union as a whole — V4.topClause applies once across all branches.
		// jOOQ's Select<R> does NOT extend SelectOrderByStep, so we cannot chain `.orderBy` directly on the
		// unionAll result. Wrap the union as a derived table when a top-clause is present; the optimizer in
		// every supported engine (DuckDB, Postgres, Redshift, ClickHouse) flattens this trivially.
		ResultQuery<Record> resultQuery;
		if (tableQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(tableQuery.getTopClause());
			resultQuery = dslContext.selectFrom(union.asTable("u"))
					.orderBy(optOrderFields)
					.limit(tableQuery.getTopClause().getLimit());
		} else {
			resultQuery = union;
		}

		// All branches share V4.filter, so their non-pushdown leftovers on the WHERE clause are identical — pick any.
		ISliceFilter sharedNonPushdown = branches.get(0).conditionAndNonPushdown().getNonPushdown();
		// Aliases of FAs that belong to more than one branch resolve to the same leftover (a function of the FA's
		// filter), so the union is well-defined; otherwise each branch contributes its own.
		Map<String, ISliceFilter> mergedAggregateLeftovers = new LinkedHashMap<>();
		branches.forEach(b -> mergedAggregateLeftovers.putAll(b.aggregatorToNonPushdown()));

		return QueryWithLeftover.builder()
				.queries(partitionQuery(resultQuery))
				.nonPushdown(sharedNonPushdown)
				.aggregatorToNonPushdowns(mergedAggregateLeftovers)
				.fields(unifiedFields)
				.build();
	}

	/**
	 * Per-branch context captured by the UNION ALL path: the V3, its {@link ISliceToJooqCondition} (reused across the
	 * branch so identical sub-filters share the cache), the shared WHERE leftover, the per-FA FILTER leftover map, and
	 * the branch's natural {@link AggregatedRecordFields}.
	 */
	protected record BranchContext(TableQueryV3 v3,
			ISliceToJooqCondition toCondition,
			ConditionWithFilter conditionAndNonPushdown,
			Map<String, ISliceFilter> aggregatorToNonPushdown,
			AggregatedRecordFields fields) {
	}

	protected BranchContext prepareBranchContext(TableQueryV3 branch) {
		ISliceToJooqCondition toCondition = makeToCondition();
		ConditionWithFilter conditionAndNonPushdown = toConditions(toCondition, branch);

		Map<String, ISliceFilter> aggregateToNonPushdown = new LinkedHashMap<>();
		branch.getAggregators().forEach(filtered -> {
			ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitNonPushdown(filtered.getFilter());
			ISliceFilter nonPushdown = conditionWithFilter.getNonPushdown();
			if (!nonPushdown.isMatchAll()) {
				aggregateToNonPushdown.put(filtered.getAlias(), nonPushdown);
			}
		});

		ImmutableSet<ISliceFilter> nonPushdowns = ImmutableSet.<ISliceFilter>builder()
				.add(conditionAndNonPushdown.getNonPushdown())
				.addAll(aggregateToNonPushdown.values())
				.build();
		AggregatedRecordFields fields = selectedColumns(branch, nonPushdowns);

		return new BranchContext(branch, toCondition, conditionAndNonPushdown, aggregateToNonPushdown, fields);
	}

	/**
	 * Compute the unified {@link AggregatedRecordFields} shape that every UNION ALL branch must project. Aggregates,
	 * groupBy columns and leftover columns are unioned by insertion order; grouping indicators cover the union of
	 * per-branch grouping indicators PLUS any groupBy column that does not appear in EVERY branch's grouping (so the
	 * row can be tagged as rolled-up when its source branch did not group by that column).
	 */
	protected AggregatedRecordFields unifyFields(List<AggregatedRecordFields> branchFields) {
		AggregatedRecordFields.AggregatedRecordFieldsBuilder unified = AggregatedRecordFields.builder();

		Set<String> aggregates = new LinkedHashSet<>();
		Set<String> columns = new LinkedHashSet<>();
		Set<String> nonPushdownCols = new LinkedHashSet<>();
		Set<String> groupingCols = new LinkedHashSet<>();
		branchFields.forEach(b -> {
			aggregates.addAll(b.getAggregates());
			columns.addAll(b.getColumns());
			nonPushdownCols.addAll(b.getNonPushdowns());
			groupingCols.addAll(b.getGroupingColumns());
		});

		// Any groupBy column not present in EVERY branch's groupBy set needs a grouping indicator across the union
		// — branches that do not carry it emit `1 AS _grp_<col>` (rolled-up) in their SELECT.
		for (String col : columns) {
			boolean inEveryBranch = branchFields.stream().allMatch(b -> b.getColumns().contains(col));
			if (!inEveryBranch) {
				groupingCols.add(col);
			}
		}

		aggregates.forEach(unified::aggregate);
		columns.forEach(unified::column);
		nonPushdownCols.forEach(unified::nonPushdown);
		groupingCols.forEach(unified::groupingColumn);
		return unified.build();
	}

	/**
	 * Build a single branch's SELECT in the unified column shape: real SQL expressions for slots the branch carries,
	 * {@code NULL} for slots it does not, and constant {@code 0}/{@code 1} for grouping indicators outside the branch's
	 * natural grouping set. The branch's WHERE / GROUP BY stay natural to its V3 so the DB still only groups/scans on
	 * what the branch actually needs.
	 */
	protected Select<Record> buildUnionBranchSelect(BranchContext branch,
			AggregatedRecordFields unified,
			TableLike<?> fromTable) {
		List<SelectFieldOrAsterisk> selectedFields = selectedUnionBranchFields(branch, unified);

		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(fromTable);
		SelectConnectByStep<Record> selectFromWhere;
		if (branch.conditionAndNonPushdown().getCondition() instanceof True) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(branch.conditionAndNonPushdown().getCondition());
		}
		return selectFromWhere
				.groupBy(makeGroupingFields(branch.v3(), branch.conditionAndNonPushdown().getNonPushdown()));
	}

	/**
	 * Build the SELECT clause for one UNION ALL branch in the unified column shape. The branch's natural slots emit the
	 * same SQL as a stand-alone V3 query would (via {@link #toSqlAggregatedColumn}, {@link #columnAsField}, or
	 * {@link DSL#grouping}); slots the branch does not carry emit {@code NULL AS <alias>} for values and {@code 0}
	 * (always-grouped) or {@code 1} (rolled-up) for grouping indicators.
	 */
	protected List<SelectFieldOrAsterisk> selectedUnionBranchFields(BranchContext branch,
			AggregatedRecordFields unified) {
		AggregatedRecordFields branchFields = branch.fields();
		ISliceToJooqCondition toCondition = branch.toCondition();
		List<SelectFieldOrAsterisk> selected = new ArrayList<>();

		// Aggregates — unified order. Branch FAs are keyed by alias; missing aliases are NULL-padded.
		Map<String, FilteredAggregator> branchAliasToFA = new LinkedHashMap<>();
		branch.v3().getAggregators().forEach(fa -> branchAliasToFA.putIfAbsent(fa.getAlias(), fa));
		boolean branchHasRealAggregate = false;
		// First pass: emit real aggregates so we know whether the branch carries any. Branches with zero real
		// aggregates (e.g. groupBy=grandTotal + only EmptyAggregation) would otherwise produce a SELECT of pure
		// constants, which the engine evaluates row-by-row rather than as an aggregating query — yielding one row
		// per source row instead of one row per group, and a downstream merge collision at the (lone) slice.
		List<SelectFieldOrAsterisk> aggregateSlots = new ArrayList<>();
		for (String alias : unified.getAggregates()) {
			FilteredAggregator fa = branchAliasToFA.get(alias);
			SelectFieldOrAsterisk sql = null;
			if (fa != null) {
				try {
					sql = toSqlAggregatedColumn(toCondition, fa);
				} catch (RuntimeException e) {
					throw new IllegalArgumentException("Issue converting to SQL: %s".formatted(fa), e);
				}
			}
			if (sql != null) {
				aggregateSlots.add(sql);
				branchHasRealAggregate = true;
			} else {
				aggregateSlots.add(null);
			}
		}
		for (int i = 0; i < unified.getAggregates().size(); i++) {
			String alias = unified.getAggregates().get(i);
			SelectFieldOrAsterisk realSlot = aggregateSlots.get(i);
			if (realSlot != null) {
				selected.add(realSlot);
			} else if (!branchHasRealAggregate) {
				// Wrap the NULL placeholder in MAX(...) so this SELECT item is itself an aggregate function. The
				// value is still NULL, but the engine now sees an aggregating query and emits one row per group.
				selected.add(DSL.max(unionNullField()).as(alias));
			} else {
				selected.add(unionNullField().as(alias));
			}
		}

		// GroupBy columns — emit the natural field aliased by its stored name for stable union-output naming;
		// NULL-pad columns this branch does not group by.
		Map<String, IAdhocColumn> branchDistinctColumns = branch.v3().getColumns();
		for (String col : unified.getColumns()) {
			if (branchFields.getColumns().contains(col)) {
				IAdhocColumn branchColumn = branchDistinctColumns.get(col);
				Objects.requireNonNull(branchColumn);
				selected.add(columnAsField(branchColumn).as(col));
			} else {
				selected.add(unionNullField().as(col));
			}
		}

		// Leftover (non-pushdown) columns — same pattern.
		for (String col : unified.getNonPushdowns()) {
			if (branchFields.getNonPushdowns().contains(col) || branchFields.getColumns().contains(col)) {
				selected.add(columnAsField(ReferencedColumn.ref(col)).as(col));
			} else {
				selected.add(unionNullField().as(col));
			}
		}

		// Grouping indicators — real GROUPING(col) when the branch's GROUPING SET produces it; constant 0
		// when the branch always groups by the column (single-groupBy branch); constant 1 when the column is
		// absent from this branch entirely (rolled-up).
		for (String col : unified.getGroupingColumns()) {
			String alias = groupingAlias(col);
			if (branchFields.getGroupingColumns().contains(col)) {
				selected.add(DSL.grouping(columnAsField(ReferencedColumn.ref(col))).as(alias));
			} else if (branchFields.getColumns().contains(col)) {
				selected.add(DSL.val(0).as(alias));
			} else {
				selected.add(DSL.val(1).as(alias));
			}
		}

		return selected;
	}

	/**
	 * NULL placeholder for UNION ALL padding. Uses a plain {@code NULL} literal rather than an explicitly-typed
	 * {@code CAST(NULL AS ...)} because UNION ALL infers each column's type from sibling branches; an explicit cast
	 * would force a concrete type and engines like DuckDB reject {@code CAST(NULL AS OTHER)} that jOOQ emits when the
	 * Java type is {@link Object} (no native SQL counterpart).
	 */
	protected Field<Object> unionNullField() {
		return DSL.field("NULL");
	}

	/**
	 * Extract ORDER BY fields from a {@link AdhocTopClause}. Mirror of {@link #getOptionalOrders(TableQueryV3)} for
	 * call sites (the UNION ALL path) that have the top clause directly rather than via a V3.
	 */
	protected List<? extends OrderField<?>> getOptionalOrders(AdhocTopClause topClause) {
		return topClause.getColumns().stream().map(c -> {
			Field<Object> field = columnAsField(c);
			SortField<Object> desc;
			if (topClause.isDesc()) {
				desc = field.desc();
			} else {
				desc = field.asc();
			}
			return desc;
		}).toList();
	}

	/**
	 * SELECT-fields builder for {@link SqlRenderMode#ROWS}: one {@code CASE WHEN <filter> THEN <col>} per FA, plus the
	 * groupBy columns as plain fields, plus any leftover columns. Empty aggregators (slice-materialization aids) are
	 * dropped — they have no SQL counterpart in row-streaming mode.
	 */
	protected Collection<SelectFieldOrAsterisk> selectedRowsFields(ISliceToJooqCondition toCondition,
			TableQueryV3 tableQuery,
			AggregatedRecordFields fields) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators().stream().distinct().forEach(filteredAggregator -> {
			Aggregator a = filteredAggregator.getAggregator();
			if (EmptyAggregation.isEmpty(a.getAggregationKey())) {
				return;
			}
			// COUNT(*) carries `*` as its `columnName`. The SLICES path handles that special-case inside
			// `DSL.count(...)`, but here in ROWS mode we are NOT aggregating — we want one row marker per
			// matching source row. Selecting `*` directly would (a) expand to every column of the underlying
			// table and (b) pollute the SELECT clause, causing JOOQ to bind the wildcard's columns to the
			// alias of COUNT(*) and any subsequent aggregator's alias. Substitute the per-row counter `1`:
			// `CASE WHEN <filter> THEN 1 END AS <alias>` — emits 1 when the FILTER matches, NULL otherwise,
			// preserving the COUNT(*) semantics at the row-streaming layer.
			Field<Object> rawColumn;
			if (ICountMeasuresConstants.ASTERISK.equals(a.getColumnName())) {
				@SuppressWarnings("unchecked")
				Field<Object> one = (Field<Object>) (Field<?>) DSL.value(1);
				rawColumn = one;
			} else {
				rawColumn = DSL.field(name(a.getColumnName()));
			}
			ConditionWithFilter faCondition = toCondition.toConditionSplitNonPushdown(filteredAggregator.getFilter());
			Field<Object> withCase = asCase(faCondition.getCondition(), rawColumn);
			selectedFields.add(withCase.as(filteredAggregator.getAlias()));
		});
		Map<String, IAdhocColumn> distinctColumns = tableQuery.getColumns();
		fields.getColumns().stream().map(distinctColumns::get).forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});
		fields.getNonPushdowns().forEach(nonPushdown -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(nonPushdown));
			selectedFields.add(field);
		});
		if (selectedFields.isEmpty()) {
			// No aggregator and no groupBy: still emit a row marker so DRILLTHROUGH counts the matching rows.
			selectedFields.add(DSL.val(1));
		}
		return selectedFields;
	}

	/**
	 * Apply ORDER BY + LIMIT to the query. Extracted so the SLICES (post-GROUP BY) and ROWS (post-WHERE) branches share
	 * the same call site even though their input types differ in JOOQ's fluent API.
	 */
	protected ResultQuery<Record> applyOrderAndLimit(ResultQuery<Record> resultQuery,
			Collection<? extends OrderField<?>> orderFields,
			Number limit) {
		if (resultQuery instanceof SelectHavingStep<Record> havingStep) {
			return havingStep.orderBy(orderFields).limit(limit);
		} else if (resultQuery instanceof SelectConnectByStep<Record> connectStep) {
			return connectStep.orderBy(orderFields).limit(limit);
		} else {
			throw new IllegalStateException(
					"Unsupported jOOQ query stage for ORDER BY/LIMIT: %s".formatted(resultQuery.getClass().getName()));
		}
	}

	protected ISliceToJooqCondition makeToCondition() {
		return sliceToCondition.with(this::name, filterOptimizer);
	}

	protected List<ResultQuery<Record>> partitionQuery(ResultQuery<Record> resultQuery) {
		return queryPartitionor.partition(resultQuery);
	}

	protected ConditionWithFilter toConditions(ISliceToJooqCondition toCondition, TableQueryV3 tableQuery) {
		Collection<Condition> conditions = new ArrayList<>();
		Collection<ISliceFilter> nonPushdownFilters = new ArrayList<>();

		// Conditions from filters
		{
			ISliceFilter filter = tableQuery.getFilter();
			ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitNonPushdown(filter);

			conditions.add(conditionWithFilter.getCondition());
			nonPushdownFilters.add(conditionWithFilter.getNonPushdown());
		}

		// AND conditions from measures and from filters
		return makeToCondition().and(conditions, nonPushdownFilters);
	}

	protected List<SelectFieldOrAsterisk> selectedSliceFields(ISliceToJooqCondition toCondition,
			TableQueryV3 tableQuery,
			AggregatedRecordFields fields) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators().stream().distinct().map(a -> {
			try {
				return toSqlAggregatedColumn(toCondition, a);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException("Issue converting to SQL: %s".formatted(a), e);
			}
		})
				// EmptyAggregation leads to no SQL aggregation
				.filter(Objects::nonNull)
				.forEach(selectedFields::add);

		// Distinct as `GROUPING SET` typically leads to a column to appear multiple times
		Map<String, IAdhocColumn> distinctColumns = tableQuery.getColumns();

		fields.getColumns().stream().map(distinctColumns::get).forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});

		// Leftover columns are also woven into GROUP BY by `makeGroupingFields` (single-groupBy non-`ALL`
		// arm and the multi-grouping-sets arm both hoist them), so adding them here in SELECT is safe.
		fields.getNonPushdowns().forEach(nonPushdown -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(nonPushdown));
			selectedFields.add(field);
		});

		// https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver17
		// https://docs.aws.amazon.com/redshift/latest/dg/r_GROUP_BY_aggregation-extensions.html#r_GROUP_BY_aggregation-extentions-grouping
		// https://neon.com/postgresql/postgresql-tutorial/postgresql-grouping-sets#grouping-function
		fields.getGroupingColumns()
				.stream()
				// alias else jooq would name `grouping` leading to ambiguities
				.map(column -> DSL.grouping(columnAsField(ReferencedColumn.ref(column))).as(groupingAlias(column)))
				.forEach(selectedFields::add);

		if (selectedFields.isEmpty()) {
			// Typically happens on EmptyAggregation on grandTotal
			// We force one field to prevent JooQ querying automatically for `*`
			// BEWARE Rely on `count(1)` and not `1`, else DuckDB considers all fields are requested, and the groupBy
			// lists all rows.
			selectedFields.add(DSL.aggregate("count", long.class, DSL.val(1)));
		}

		return selectedFields;
	}

	protected AggregatedRecordFields selectedColumns(TableQueryV3 tableQuery, Set<ISliceFilter> leftovers) {
		return QueryWithLeftover.makeSelectedColumns(tableQuery, leftovers);
	}

	/**
	 * Most usages are not groupBy.
	 *
	 * @param column
	 * @return a {@link Field} mapping to given column.
	 */
	protected Field<Object> columnAsField(IAdhocColumn column) {
		String columnName = column.getName();
		Field<Object> field;

		if (column instanceof IHasSqlExpression hasSql) {
			// TODO How could we transcode column referred by the SQL?
			// Should we add named columns from transcoder?
			String sql = hasSql.getSql();
			field = DSL.field(sql).as(columnName);
		} else {
			Field<Object> unaliasedField = DSL.field(name(columnName));

			// GroupBy: refer to the underlying column, to prevent ambiguities
			// If we were to have some aliasing around here, aliases should probably not be applied on groupBy
			// https://github.com/duckdb/duckdb/issues/16097
			// https://github.com/jOOQ/jOOQ/issues/17980
			field = unaliasedField;
		}
		return field;
	}

	/**
	 *
	 * @param name
	 *            may be a simple columnName (e.g. `someField`), or a joined field (e.g. `someTable.someField`),or a
	 *            qualified name (e.g. `"someTable.someField"`).
	 * @return
	 */
	protected Name name(String name) {
		return AdhocJooqHelper.name(name, dslContext::parser);
	}

	/**
	 *
	 * @param tableQuery
	 * @param nonPushdownFilter
	 *            the filter which has not been able to be transcoded into a {@link Condition}
	 * @return
	 */
	protected Collection<GroupField> makeGroupingFields(TableQueryV3 tableQuery, ISliceFilter nonPushdownFilter) {
		List<GroupField> groupedFields = new ArrayList<>();
		if (tableQuery.singleGroupBy().isPresent()) {
			if (canGroupByAll()) {
				// `GROUP BY ALL` is supported by: DuckDB, RedShift, More?
				// https://duckdb.org/docs/stable/sql/query_syntax/groupby.html#group-by-all
				// https://docs.aws.amazon.com/redshift/latest/dg/r_GROUP_BY_clause.html
				groupedFields.add(DSL.field(DSL.unquotedName("ALL")));
			} else {
				tableQuery.getColumns().values().forEach(column -> {
					Field<Object> field = columnAsField(column);
					groupedFields.add(field);
				});

				FilterHelpers.getFilteredColumns(nonPushdownFilter).forEach(column -> {
					Field<Object> field = columnAsField(ReferencedColumn.ref(column));
					groupedFields.add(field);
				});
			}
		} else {
			// At least 2 groupingSets. Hoist nonPushdown columns into each grouping set individually (only when
			// not already present) so each row's keyset still identifies its original groupBy; downstream
			// projection then strips the hoisted columns back out.
			Set<String> nonPushdownColumns = FilterHelpers.getFilteredColumns(nonPushdownFilter);

			List<? extends List<? extends Field<?>>> fields2 = tableQuery.streamGroupBy().map(gb -> {
				Set<String> gbColumns = gb.getSortedColumns();
				List<Field<?>> gbFields = new ArrayList<>();
				gb.getColumns().forEach(c -> gbFields.add(columnAsField(c)));
				nonPushdownColumns.stream()
						.filter(c -> !gbColumns.contains(c))
						.forEach(c -> gbFields.add(columnAsField(ReferencedColumn.ref(c))));
				return gbFields;
			}).toList();

			Collection<? extends Field<?>>[] fieldSets = fields2.toArray(List[]::new);

			groupedFields.add(DSL.groupingSets(fieldSets));
		}

		return groupedFields;
	}

	protected boolean canGroupByAll() {
		return capabilities.isAbleToGroupByAll();
	}

	protected boolean canFilterAggregates() {
		return capabilities.isAbleToFilterAggregates();
	}

	protected List<? extends OrderField<?>> getOptionalOrders(TableQueryV3 tableQuery) {
		return getOptionalOrders(tableQuery.getTopClause());
	}

	protected @Nullable SelectFieldOrAsterisk toSqlAggregatedColumn(ISliceToJooqCondition toCondition,
			FilteredAggregator filteredAggregator) {
		Aggregator a = filteredAggregator.getAggregator();

		String aggregationKey = a.getAggregationKey();
		if (EmptyAggregation.isEmpty(aggregationKey)) {
			// There is no aggregation for empty: we just want to fetch groupBys
			return null;
		}

		String columnName = a.getColumnName();

		if (ExpressionAggregation.isExpression(aggregationKey)) {
			return buildExpressionField(columnName, filteredAggregator);
		}

		return buildAggregateField(toCondition, filteredAggregator, columnName);
	}

	protected SelectFieldOrAsterisk buildExpressionField(String columnName, FilteredAggregator filteredAggregator) {
		// Do not call `name` to make sure it is not qualified
		Field<?> unaliasedField = DSL.field(DSL.sql(columnName));

		if (!filteredAggregator.getFilter().isMatchAll()) {
			// BEWARE It is unclear how this could be managed: how to help TableQueryV3 producing valid FILTERs?
			throw new NotYetImplementedException(
					"FILTER with `ExpressionAggregation` is not managed. filteredAggregator=" + filteredAggregator);
		}

		return unaliasedField.as(filteredAggregator.getAlias());
	}

	protected SelectFieldOrAsterisk buildAggregateField(ISliceToJooqCondition toCondition,
			FilteredAggregator filteredAggregator,
			String columnName) {
		Name namedColumn = name(columnName);

		ConditionWithFilter condition = toCondition.toConditionSplitNonPushdown(filteredAggregator.getFilter());
		if (!condition.getNonPushdown().isMatchAll()) {
			log.debug("FILTER with a nonPushdown. filter={}",
					PepperLogHelper.getObjectAndClass(filteredAggregator.getFilter()));
		}

		Condition conditionInCase = buildConditionInCase(condition);

		Aggregator a = filteredAggregator.getAggregator();
		String aggregationKey = a.getAggregationKey();

		Field<Object> fieldWithoutCase = DSL.field(namedColumn);
		AggregateFunction<?> sqlAggFunction =
				buildAggregateFunction(aggregationKey, a, namedColumn, fieldWithoutCase, conditionInCase);

		Field<?> unaliasedField = applyFilterCondition(condition, sqlAggFunction);

		return unaliasedField.as(filteredAggregator.getAlias());
	}

	protected Condition buildConditionInCase(ConditionWithFilter condition) {
		boolean needCase = !(condition.getCondition() instanceof True) && !canFilterAggregates();
		if (needCase) {
			return condition.getCondition();
		}
		return DSL.trueCondition();
	}

	protected AggregateFunction<?> buildAggregateFunction(String aggregationKey,
			Aggregator a,
			Name namedColumn,
			Field<Object> fieldWithoutCase,
			Condition conditionInCase) {
		Field<Object> fieldToAggregate = asCase(conditionInCase, fieldWithoutCase);

		// TODO How not to define the output type from here (e.g. accept BigInteger or `double`, as would be
		// outputed by DuckDB)
		// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
		if (SumAggregation.KEY.equals(aggregationKey)) {
			return aggregate("sum", fieldToAggregate);
		} else if (MaxAggregation.KEY.equals(aggregationKey)) {
			return DSL.max(fieldToAggregate);
		} else if (MinAggregation.KEY.equals(aggregationKey)) {
			return DSL.min(fieldToAggregate);
		} else if (AvgAggregation.isAvg(aggregationKey)) {
			return aggregate("avg", fieldToAggregate);
		} else if (CountAggregation.isCount(aggregationKey)) {
			return buildCountAggregate(fieldWithoutCase, conditionInCase);
		} else if (RankAggregation.isRank(aggregationKey)) {
			return buildRankAggregate(a, fieldToAggregate);
		} else if (CoalesceAggregation.KEY.equals(aggregationKey)) {
			// `CoalesceAggregation` ("the column is constant for the slice — return any one value") maps to
			// `any_value(col)` (DuckDB / standard SQL since 2023): same row-preserving guarantee, no
			// double-counting risk. The DRILLTHROUGH path no longer relies on this (it bypasses GROUP BY via
			// `streamRawRows`), but the mapping stays valid for any caller emitting Coalesce in a regular query.
			return aggregate("any_value", fieldToAggregate);
		} else {
			return onCustomAggregation(a, namedColumn, conditionInCase);
		}
	}

	protected AggregateFunction<?> buildCountAggregate(Field<Object> fieldWithoutCase, Condition conditionInCase) {
		if (fieldWithoutCase.equals(asCase(conditionInCase, fieldWithoutCase))
		// || !DSL.name("*").equals(fieldWithoutCase)
		) {
			// No case/filter
			return DSL.count(fieldWithoutCase);
		}

		// Case: rewrap ensuring this is wrapped with `COUNT(CASE ... THEN 1)`
		Field<?> fieldAs1 = DSL.field(DSL.value(1));
		Field<?> caseOnFieldAs1 = asCase(conditionInCase, fieldAs1);
		return DSL.count(caseOnFieldAs1);
	}

	protected AggregateFunction<?> buildRankAggregate(Aggregator a, Field<Object> fieldToAggregate) {
		RankAggregation agg = (RankAggregation) operatorFactory.makeAggregation(a);
		String duckDbFunction;

		if (agg.isAscElseDesc()) {
			duckDbFunction = "arg_min";
		} else {
			duckDbFunction = "arg_max";
		}

		// https://duckdb.org/docs/stable/sql/functions/aggregates.html#arg_maxarg-val-n
		Name functionName = DSL.systemName(duckDbFunction);
		Param<Integer> rank = DSL.val(agg.getRank());
		return DSL.aggregate(functionName, Object.class, fieldToAggregate, fieldToAggregate, rank);
	}

	protected Field<?> applyFilterCondition(ConditionWithFilter condition, AggregateFunction<?> sqlAggFunction) {
		if (condition.getCondition() instanceof True) {
			return sqlAggFunction;
		}

		boolean needCase = !(condition.getCondition() instanceof True) && !canFilterAggregates();
		if (needCase) {
			return sqlAggFunction;
		}

		return sqlAggFunction.filterWhere(condition.getCondition());
	}

	// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
	protected AggregateFunction<Object> aggregate(String aggregationFunction, Field<Object> field) {
		return DSL.aggregate(DSL.systemName(aggregationFunction), field.getDataType(), field);
	}

	protected <T> Field<T> asCase(Condition condition, Field<T> field) {
		if (condition instanceof True) {
			return field;
		} else {
			return DSL.when(condition, field);
		}
	}

	/**
	 * 
	 * @param aggregator
	 * @param namedColumn
	 * @param condition
	 *            a condition to be applied. May be a `TrueCondition` if there is no {@link Condition} to apply.
	 * @return
	 */
	protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator, Name namedColumn, Condition condition) {
		Field<Object> fieldWithoutCase = DSL.field(namedColumn);
		Field<Object> fieldToAggregate = asCase(condition, fieldWithoutCase);

		return onCustomAggregation(aggregator, fieldToAggregate);
	}

	protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator, Field<Object> fieldToAggregate) {
		String aggregationKey = aggregator.getAggregationKey();

		// TODO Could we prefer some generic aggregation? (e.g. `array_agg` in DuckDB)
		throw new UnsupportedOperationException("SQL does not support aggregationKey=%s".formatted(aggregationKey));
	}

	@Deprecated(since = "TODO Migrate unitTests")
	public QueryWithLeftover prepareSliceQuery(TableQuery tableQuery) {
		return prepareSliceQuery(TableQueryV3.edit(tableQuery).build());
	}

	public static String groupingAlias(String c) {
		return "grouping_" + c.replaceAll("[\".]", "") + "_";
	}

}
