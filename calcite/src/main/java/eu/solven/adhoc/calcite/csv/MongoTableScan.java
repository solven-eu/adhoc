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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;

/**
 * Relational expression representing a scan of a MongoDB collection.
 *
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate" methods.
 */
public class MongoTableScan extends TableScan implements MongoRel {
	final MongoTable mongoTable;
	final @Nullable RelDataType projectRowType;

	/**
	 * Creates a MongoTableScan.
	 *
	 * @param cluster
	 *            Cluster
	 * @param traitSet
	 *            Traits
	 * @param table
	 *            Table
	 * @param mongoTable
	 *            MongoDB table
	 * @param projectRowType
	 *            Fields and types to project; null to project raw row
	 */
	protected MongoTableScan(RelOptCluster cluster,
			RelTraitSet traitSet,
			RelOptTable table,
			MongoTable mongoTable,
			@Nullable RelDataType projectRowType) {
		super(cluster, traitSet, ImmutableList.of(), table);
		this.mongoTable = requireNonNull(mongoTable, "mongoTable");
		this.projectRowType = projectRowType;
		checkArgument(getConvention() == MongoRel.CONVENTION);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		assert inputs.isEmpty();
		return this;
	}

	@Override
	public RelDataType deriveRowType() {
		if (projectRowType != null) {
			return projectRowType;
		} else {
			return super.deriveRowType();
		}
	}

	@Override
	public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		// scans with a small project list are cheaper
		final float f = projectRowType == null ? 1f : projectRowType.getFieldCount() / 100f;
		final RelOptCost cost = requireNonNull(super.computeSelfCost(planner, mq));
		return cost.multiplyBy(.1 * f);
	}

	@Override
	public void register(RelOptPlanner planner) {
		planner.addRule(MongoToEnumerableConverterRule.INSTANCE);
		for (RelOptRule rule : MongoRules.RULES) {
			planner.addRule(rule);
		}
	}

	@Override
	public void implement(Implementor implementor) {
		implementor.mongoTable = mongoTable;
		implementor.table = table;
	}
}