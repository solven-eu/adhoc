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

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table based on a CSV file.
 */
public class CsvTranslatableTable extends CsvTable implements QueryableTable, TranslatableTable {
	/** Creates a CsvTable. */
	CsvTranslatableTable(Source source, @Nullable RelProtoDataType protoRowType) {
		super(source, protoRowType);
	}

	@Override
	public String toString() {
		return "CsvTranslatableTable";
	}

	/** Returns an enumerable over a given projection of the fields. */
	@SuppressWarnings("unused") // called from generated code
	public Enumerable<Object> project(final DataContext root, final int[] fields) {
		final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
		return new AbstractEnumerable<Object>() {
			@Override
			public Enumerator<Object> enumerator() {
				JavaTypeFactory typeFactory = root.getTypeFactory();
				return new CsvEnumerator<>(source, cancelFlag, getFieldTypes(typeFactory), ImmutableIntList.of(fields));
			}
		};
	}

	@Override
	public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
		return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
	}

	@Override
	public Type getElementType() {
		return Object[].class;
	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		// Request all fields.
		final int fieldCount = relOptTable.getRowType().getFieldCount();
		final int[] fields = CsvEnumerator.identityList(fieldCount);
		return new CsvTableScan(context.getCluster(), relOptTable, this, fields);
	}
}
