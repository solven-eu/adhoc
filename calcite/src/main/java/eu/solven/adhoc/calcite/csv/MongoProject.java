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
import java.util.Locale;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project} relational expression in MongoDB.
 */
public class MongoProject extends Project implements MongoRel {
	public MongoProject(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			List<? extends RexNode> projects,
			RelDataType rowType) {
		super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
		assert getConvention() == MongoRel.CONVENTION;
		assert getConvention() == input.getConvention();
	}

	@Deprecated // to be removed before 2.0
	public MongoProject(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			List<RexNode> projects,
			RelDataType rowType,
			int flags) {
		this(cluster, traitSet, input, projects, rowType);
		Util.discard(flags);
	}

	@Override
	public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
		return new MongoProject(getCluster(), traitSet, input, projects, rowType);
	}

	@Override
	public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		return super.computeSelfCost(planner, mq).multiplyBy(0.1);
	}

	@Override
	public void implement(Implementor implementor) {
		implementor.visitChild(0, getInput());

		final MongoRules.RexToMongoTranslator translator =
				new MongoRules.RexToMongoTranslator((JavaTypeFactory) getCluster().getTypeFactory(),
						MongoRules.mongoFieldNames(getInput().getRowType()));
		final List<String> items = new ArrayList<>();
		for (Pair<RexNode, String> pair : getNamedProjects()) {
			final String name = pair.right;
			final String expr = pair.left.accept(translator);
			boolean isSimple = expr.toLowerCase(Locale.US).equals(("'$" + name + "'").toLowerCase(Locale.US));

			if (isSimple) {
				implementor.adhocQueryBuilder.groupByAlso(name);
				// items.add(isSimple ? MongoRules.maybeQuote(name) + ": 1" : MongoRules.maybeQuote(name) + ": " +
				// expr);
			} else {
				throw new UnsupportedOperationException("Issue with %s %s".formatted(name, expr));
			}

		}
		// final String findString = Util.toString(items, "{", ", ", "}");
		// final String aggregateString = "{$project: " + findString + "}";
		// final Pair<String, String> op = Pair.of(findString, aggregateString);
		// // implementor.add(op.left, op.right);
		// throw new UnsupportedOperationException("TODO");
	}
}