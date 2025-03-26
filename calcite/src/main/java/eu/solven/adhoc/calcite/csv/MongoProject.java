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

import java.util.List;

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
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project} relational expression in Adhoc.
 */
@Slf4j
@Deprecated
public class MongoProject extends Project implements AdhocCalciteRel {
	public MongoProject(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			List<? extends RexNode> projects,
			RelDataType rowType) {
		super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
		assert getConvention() == AdhocCalciteRel.CONVENTION;
		assert getConvention() == input.getConvention();
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
	public void implement(AdhocCalciteRelImplementor implementor) {
		implementor.visitChild(0, getInput());

		final MongoRules.RexToMongoTranslator translator =
				new MongoRules.RexToMongoTranslator((JavaTypeFactory) getCluster().getTypeFactory(),
						MongoRules.mongoFieldNames(getInput().getRowType()));

		// Clear previous named projects
		implementor.clearProject();
		for (Pair<RexNode, String> pair : getNamedProjects()) {
			final String name = pair.right;
			final String expr = pair.left.accept(translator);

			if (!name.equals(expr)) {
				// throw new UnsupportedOperationException("Issue with name=%s expr=%s".formatted(name, expr));
				log.warn("Not trivial project: {] -> {}", name, expr);
			}
			implementor.projects.put(name, expr);
		}
	}
}