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
package eu.solven.adhoc.calcite.csv;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate} relational expression in MongoDB.
 */
public class AdhocCalciteAggregate extends Aggregate implements AdhocCalciteRel {
	public AdhocCalciteAggregate(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			ImmutableBitSet groupSet,
			@Nullable List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) throws InvalidRelException {
		super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
		assert getConvention() == AdhocCalciteRel.CONVENTION;
		assert getConvention() == input.getConvention();

		// for (AggregateCall aggCall : aggCalls) {
		// if (aggCall.isDistinct()) {
		// throw new InvalidRelException("distinct aggregation not supported");
		// }
		// }
		switch (getGroupType()) {
		case SIMPLE:
			break;
		default:
			throw new InvalidRelException("unsupported group type: " + getGroupType());
		}
	}

	@Deprecated // to be removed before 2.0
	public AdhocCalciteAggregate(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			boolean indicator,
			ImmutableBitSet groupSet,
			List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) throws InvalidRelException {
		this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
		checkIndicator(indicator);
	}

	@Override
	public Aggregate copy(RelTraitSet traitSet,
			RelNode input,
			ImmutableBitSet groupSet,
			@Nullable List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) {
		try {
			return new AdhocCalciteAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
		} catch (InvalidRelException e) {
			// Semantic error not possible. Must be a bug. Convert to
			// internal error.
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void implement(AdhocCalciteRelImplementor implementor) {
		implementor.visitChild(0, getInput());

		List<String> groupByColumns = new ArrayList<>();

		// final List<String> inNames = MongoRules.mongoFieldNames(getInput().getRowType());
		// final List<String> outNames = MongoRules.mongoFieldNames(getRowType());

		int i = 0;
		for (int group : groupSet) {
			groupByColumns.add(getInput().getRowType().getFieldNames().get(group));
			++i;
		}

		List<String> measures = new ArrayList<>();
		List<Aggregator> aggregators = new ArrayList<>();

		for (Pair<AggregateCall, String> namedAggCall : getNamedAggCalls()) {
			AggregateCall aggCall = namedAggCall.getKey();

			String outName = getRowType().getFieldNames().get(i++);

			String aggCallName = aggCall.getName();
			if (aggCallName == null) {
				// TODO Unclear when/why this happens
				aggCallName = namedAggCall.getValue();
			}
			aggCallName = outName;

			if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
				if (aggCall.getArgList().isEmpty()) {
					aggregators.add(Aggregator.builder()
							.name(aggCallName)
							.aggregationKey(CountAggregation.KEY)
							.columnName(ICountMeasuresConstants.ASTERISK)
							.build());
				} else {
					if (aggCall.getArgList().size() >= 2) {
						throw new IllegalArgumentException(
								"Can not COUNT more than 1 column: %s".formatted(aggCall.getArgList()));
					}

					String countedField = getInput().getRowType().getFieldNames().get(aggCall.getArgList().getFirst());
					// COUNT only when given column is not null
					aggregators.add(Aggregator.builder()
							.name(aggCallName)
							.aggregationKey(CountAggregation.KEY)
							.columnName(countedField)
							.build());
				}
			} else if (aggCall.getAggregation() instanceof SqlSumAggFunction
					|| aggCall.getAggregation() instanceof SqlSumEmptyIsZeroAggFunction
			// aggCall.getAggregation().getKind() == SqlKind.SUM
			// || aggCall.getAggregation().getKind() == SqlKind.SUM0
			) {
				if (aggCall.getArgList().size() != 1) {
					throw new IllegalArgumentException(
							"Can not SUM other than 1 column: %s".formatted(aggCall.getArgList()));
				}
				int index = aggCall.getArgList().getFirst();
				String summedField = getInput().getRowType().getFieldList().get(index).getName();
				if (summedField.startsWith("$f")) {
					// org.apache.calcite.rex.RexUtil.createStructType(RelDataTypeFactory, List<? extends RexNode>,
					// List<? extends String>, Suggester)
					summedField = implementor.projects.get(summedField);
				}
				aggregators.add(Aggregator.builder()
						.name(aggCallName)
						.aggregationKey(SumAggregation.KEY)
						.columnName(summedField)
						.build());
			} else if (aggCall.getAggregation().getKind() == SqlKind.AVG) {
				String aggregationKey = AvgAggregation.KEY;
				aggregators.add(aggregateOverSingleField(implementor, aggCall, aggCallName, aggregationKey));
			} else if (aggCall.getAggregation().getKind() == SqlKind.MIN) {
				String aggregationKey = MinAggregation.KEY;
				aggregators.add(aggregateOverSingleField(implementor, aggCall, aggCallName, aggregationKey));
			} else if (aggCall.getAggregation().getKind() == SqlKind.MAX) {
				String aggregationKey = MaxAggregation.KEY;
				aggregators.add(aggregateOverSingleField(implementor, aggCall, aggCallName, aggregationKey));
			} else {
				throw new IllegalArgumentException("Not managed: %s".formatted(aggCall.getAggregation().getKind()));
				// measures.add(aggCallName);
			}
		}

		implementor.adhocQueryBuilder
				// Some measures are referred by their name
				.measureNames(measures)
				// Some measures are built on the fly
				.measures(aggregators)
				.groupBy(GroupByColumns.named(groupByColumns));
	}

	protected Aggregator aggregateOverSingleField(AdhocCalciteRelImplementor implementor,
			AggregateCall aggCall,
			String aggCallName,
			String aggregationKey) {
		if (aggCall.getArgList().size() != 1) {
			throw new IllegalArgumentException(
					"Can not %s other than 1 column: %s".formatted(aggregationKey, aggCall.getArgList()));
		}
		int index = aggCall.getArgList().getFirst();
		String avgedField = getInput().getRowType().getFieldList().get(index).getName();
		if (avgedField.startsWith("$f")) {
			// org.apache.calcite.rex.RexUtil.createStructType(RelDataTypeFactory, List<? extends RexNode>,
			// List<? extends String>, Suggester)
			avgedField = implementor.projects.get(avgedField);
		}
		return Aggregator.builder().name(aggCallName).aggregationKey(aggregationKey).columnName(avgedField).build();
	}

	private static String toMongo(SqlAggFunction aggregation, List<String> inNames, List<Integer> args) {
		if (aggregation == SqlStdOperatorTable.COUNT) {
			if (args.isEmpty()) {
				return "{$sum: 1}";
			} else {
				assert args.size() == 1;
				final String inName = inNames.get(args.get(0));
				return "{$sum: {$cond: [ {$eq: [" + MongoRules.quote(inName) + ", null]}, 0, 1]}}";
			}
		} else if (aggregation instanceof SqlSumAggFunction || aggregation instanceof SqlSumEmptyIsZeroAggFunction) {
			assert args.size() == 1;
			final String inName = inNames.get(args.get(0));
			return "{$sum: " + MongoRules.maybeQuote("$" + inName) + "}";
		} else if (aggregation == SqlStdOperatorTable.MIN) {
			assert args.size() == 1;
			final String inName = inNames.get(args.get(0));
			return "{$min: " + MongoRules.maybeQuote("$" + inName) + "}";
		} else if (aggregation == SqlStdOperatorTable.MAX) {
			assert args.size() == 1;
			final String inName = inNames.get(args.get(0));
			return "{$max: " + MongoRules.maybeQuote("$" + inName) + "}";
		} else if (aggregation == SqlStdOperatorTable.AVG) {
			assert args.size() == 1;
			final String inName = inNames.get(args.get(0));
			return "{$avg: " + MongoRules.maybeQuote("$" + inName) + "}";
		} else {
			throw new AssertionError("unknown aggregate " + aggregation);
		}
	}
}