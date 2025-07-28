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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter} relational expression in MongoDB.
 */
public class MongoFilter extends Filter implements AdhocCalciteRel {
	public MongoFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
		super(cluster, traitSet, child, condition);
		assert getConvention() == AdhocCalciteRel.CONVENTION;
		assert getConvention() == child.getConvention();
	}

	@Override
	public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		return super.computeSelfCost(planner, mq).multiplyBy(0.1);
	}

	@Override
	public MongoFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
		return new MongoFilter(getCluster(), traitSet, input, condition);
	}

	@Override
	public void implement(AdhocCalciteRelImplementor implementor) {
		implementor.visitChild(0, getInput());
		Translator translator = new Translator(implementor.rexBuilder, MongoRules.mongoFieldNames(getRowType()));
		ISliceFilter match = translator.translateMatch(condition);
		// implementor.add(null, match);
		implementor.cubeQueryBuilder.andFilter(match);
	}

	/** Translates {@link RexNode} expressions into MongoDB expression strings. */
	static class Translator {
		final Multimap<String, Pair<String, RexLiteral>> multimap = HashMultimap.create();
		final Map<String, RexLiteral> eqMap = new LinkedHashMap<>();
		private final RexBuilder rexBuilder;
		private final List<String> fieldNames;

		Translator(RexBuilder rexBuilder, List<String> fieldNames) {
			this.rexBuilder = rexBuilder;
			this.fieldNames = fieldNames;
		}

		private ISliceFilter translateMatch(RexNode condition) {
			return translateOr(condition);
		}

		private ISliceFilter translateOr(RexNode condition) {
			final RexNode condition2 = RexUtil.expandSearch(rexBuilder, null, condition);

			List<ISliceFilter> listToOr = new ArrayList<>();
			for (RexNode node : RelOptUtil.disjunctions(condition2)) {
				listToOr.add(AndFilter.and(translateAnd(node)));
			}

			return OrFilter.or(listToOr);
		}

		/**
		 * Translates a condition that may be an AND of other conditions. Gathers together conditions that apply to the
		 * same field.
		 */
		private Map<String, Object> translateAnd(RexNode node0) {
			eqMap.clear();
			multimap.clear();
			for (RexNode node : RelOptUtil.conjunctions(node0)) {
				translateMatch2(node);
			}
			Map<String, Object> map = new LinkedHashMap<>();
			for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
				multimap.removeAll(entry.getKey());
				map.put(entry.getKey(), literalValue(entry.getValue()));
			}
			for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry : multimap.asMap().entrySet()) {
				List<IValueMatcher> matchers = new ArrayList<>();
				for (Pair<String, RexLiteral> s : entry.getValue()) {
					matchers.add(addPredicate(s.left, literalValue(s.right)));
				}
				map.put(entry.getKey(), OrMatcher.or(matchers));
			}
			return map;
		}

		private static IValueMatcher addPredicate(String op, Object v) {
			if ("$gt".equals(op)) {
				return ComparingMatcher.builder()
						.greaterThan(true)
						.matchIfEqual(false)
						.matchIfNull(false)
						.operand(v)
						.build();
			} else {
				throw new IllegalArgumentException("Not managed: %s".formatted(op));
			}
		}

		/**
		 * Returns whether {@code v0} is a stronger value for operator {@code key} than {@code v1}.
		 *
		 * <p>
		 * For example, {@code stronger("$lt", 100, 200)} returns true, because "&lt; 100" is a more powerful condition
		 * than "&lt; 200".
		 */
		private static boolean stronger(String key, Object v0, Object v1) {
			if ("$lt".equals(key) || "$lte".equals(key)) {
				if (v0 instanceof Number && v1 instanceof Number) {
					return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
				}
				if (v0 instanceof String && v1 instanceof String) {
					return v0.toString().compareTo(v1.toString()) < 0;
				}
			}
			if ("$gt".equals(key) || "$gte".equals(key)) {
				return stronger("$lt", v1, v0);
			}
			return false;
		}

		private static Object literalValue(RexLiteral literal) {
			return literal.getValue2();
		}

		private Void translateMatch2(RexNode node) {
			switch (node.getKind()) {
			case EQUALS:
				return translateBinary(null, null, (RexCall) node);
			case LESS_THAN:
				return translateBinary("$lt", "$gt", (RexCall) node);
			case LESS_THAN_OR_EQUAL:
				return translateBinary("$lte", "$gte", (RexCall) node);
			case NOT_EQUALS:
				return translateBinary("$ne", "$ne", (RexCall) node);
			case GREATER_THAN:
				return translateBinary("$gt", "$lt", (RexCall) node);
			case GREATER_THAN_OR_EQUAL:
				return translateBinary("$gte", "$lte", (RexCall) node);
			default:
				throw new AssertionError("cannot translate " + node);
			}
		}

		/**
		 * Translates a call to a binary operator, reversing arguments if necessary.
		 */
		private Void translateBinary(String op, String rop, RexCall call) {
			final RexNode left = call.operands.get(0);
			final RexNode right = call.operands.get(1);
			boolean b = translateBinary2(op, left, right);
			if (b) {
				return null;
			}
			b = translateBinary2(rop, right, left);
			if (b) {
				return null;
			}
			throw new AssertionError("cannot translate op " + op + " call " + call);
		}

		/** Translates a call to a binary operator. Returns whether successful. */
		private boolean translateBinary2(String op, RexNode left, RexNode right) {
			switch (right.getKind()) {
			case LITERAL:
				break;
			default:
				return false;
			}
			final RexLiteral rightLiteral = (RexLiteral) right;
			switch (left.getKind()) {
			case INPUT_REF:
				final RexInputRef left1 = (RexInputRef) left;
				String name = fieldNames.get(left1.getIndex());
				translateOp2(op, name, rightLiteral);
				return true;
			case CAST:
				return translateBinary2(op, ((RexCall) left).operands.get(0), right);
			case ITEM:
				String itemName = MongoRules.isItem((RexCall) left);
				if (itemName != null) {
					translateOp2(op, itemName, rightLiteral);
					return true;
				}
				// fall through
			default:
				return false;
			}
		}

		private void translateOp2(String op, String name, RexLiteral right) {
			if (op == null) {
				// E.g.: {deptno: 100}
				eqMap.put(name, right);
			} else {
				// E.g. {deptno: {$lt: 100}}
				// which may later be combined with other conditions:
				// E.g. {deptno: [$lt: 100, $gt: 50]}
				multimap.put(name, Pair.of(op, right));
			}
		}
	}
}