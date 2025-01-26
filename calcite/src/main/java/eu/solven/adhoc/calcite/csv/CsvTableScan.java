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

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;

/**
 * Relational expression representing a scan of a CSV file.
 *
 * <p>
 * Like any table scan, it serves as a leaf node of a query tree.
 */
public class CsvTableScan extends TableScan implements EnumerableRel {
	final CsvTranslatableTable csvTable;
	final int[] fields;

	protected CsvTableScan(RelOptCluster cluster, RelOptTable table, CsvTranslatableTable csvTable, int[] fields) {
		super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
		this.csvTable = requireNonNull(csvTable, "csvTable");
		this.fields = fields;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		assert inputs.isEmpty();
		return new CsvTableScan(getCluster(), table, csvTable, fields);
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw).item("fields", Primitive.asList(fields));
	}

	@Override
	public RelDataType deriveRowType() {
		final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
		final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
		for (int field : fields) {
			builder.add(fieldList.get(field));
		}
		return builder.build();
	}

	@Override
	public void register(RelOptPlanner planner) {
		// planner.addRule(CsvRules.PROJECT_SCAN);
	}

	@Override
	public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		// Multiply the cost by a factor that makes a scan more attractive if it
		// has significantly fewer fields than the original scan.
		//
		// The "+ 2D" on top and bottom keeps the function fairly smooth.
		//
		// For example, if table has 3 fields, project has 1 field,
		// then factor = (1 + 2) / (3 + 2) = 0.6
		final RelOptCost cost = requireNonNull(super.computeSelfCost(planner, mq));
		return cost.multiplyBy((fields.length + 2D) / (table.getRowType().getFieldCount() + 2D));
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

		return implementor.result(physType,
				Blocks.toBlock(Expressions.call(requireNonNull(table.getExpression(CsvTranslatableTable.class)),
						"project",
						implementor.getRootExpression(),
						Expressions.constant(fields))));
	}
}
