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

import java.io.UncheckedIOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.query.AdhocQuery;

/**
 * Relational expression representing a scan of a table in a Mongo data source.
 */
public class MongoToEnumerableConverter extends ConverterImpl implements EnumerableRel {
	protected MongoToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		super(cluster, ConventionTraitDef.INSTANCE, traits, input);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new MongoToEnumerableConverter(getCluster(), traitSet, sole(inputs));
	}

	@Override
	public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		return super.computeSelfCost(planner, mq).multiplyBy(.1);
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		// Generates a call to "find" or "aggregate", depending upon whether
		// an aggregate is present.
		//
		// ((MongoTable) schema.getTable("zips")).find(
		// "{state: 'CA'}",
		// "{city: 1, zipcode: 1}")
		//
		// ((MongoTable) schema.getTable("zips")).aggregate(
		// "{$filter: {state: 'CA'}}",
		// "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: "$pop"}}")
		final BlockBuilder list = new BlockBuilder();
		final AdhocCalciteRel.AdhocImplementor adhocImplementor =
				new AdhocCalciteRel.AdhocImplementor(getCluster().getRexBuilder());
		adhocImplementor.visitChild(0, getInput());
		final RelDataType rowType = getRowType();
		final PhysType physType =
				PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
		final Expression fields = list.append("fields",
				constantArrayList(Pair.zip(MongoRules.mongoFieldNames(rowType), new AbstractList<Class>() {
					@Override
					public Class get(int index) {
						return physType.fieldClass(index);
					}

					@Override
					public int size() {
						return rowType.getFieldCount();
					}
				}), Pair.class));
		final Expression table =
				list.append("table", adhocImplementor.table.getExpression(MongoTable.MongoQueryable.class));
		// List<String> opList = mongoImplementor.list.rightList();
		AdhocQuery adhocQuery = adhocImplementor.adhocQueryBuilder.build();
		String queryAsString;
		try {
			queryAsString = new ObjectMapper().writeValueAsString(adhocQuery);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}

		final Expression ops = list.append("ops", constantArrayList(Arrays.asList(queryAsString), Object.class));

		Expression enumerable = list.append("enumerable",
				Expressions.call(table, MongoMethod.MONGO_QUERYABLE_AGGREGATE.method, fields, ops));
		// Hook.QUERY_PLAN.run(opList);
		list.add(Expressions.return_(null, enumerable));
		return implementor.result(physType, list.toBlock());
	}

	/**
	 * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
	 *
	 * @param values
	 *            List of values
	 * @param clazz
	 *            Type of values
	 * @return expression
	 */
	private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
		return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
				Expressions.newArrayInit(clazz, constantList(values)));
	}

	/**
	 * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
	 */
	private static <T> List<Expression> constantList(List<T> values) {
		return Util.transform(values, Expressions::constant);
	}
}