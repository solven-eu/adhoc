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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table based on a CSV file.
 *
 * <p>
 * It implements the {@link ScannableTable} interface, so Calcite gets data by calling the {@link #scan(DataContext)}
 * method.
 */
public class CsvScannableTable extends CsvTable implements ScannableTable {
	/** Creates a CsvScannableTable. */
	CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType) {
		super(source, protoRowType);
	}

	@Override
	public String toString() {
		return "CsvScannableTable";
	}

	@Override
	public Enumerable<@Nullable Object[]> scan(DataContext root) {
		JavaTypeFactory typeFactory = root.getTypeFactory();
		final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
		final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
		final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
		return new AbstractEnumerable<@Nullable Object[]>() {
			@Override
			public Enumerator<@Nullable Object[]> enumerator() {
				return new CsvEnumerator<>(source,
						cancelFlag,
						false,
						null,
						CsvEnumerator.arrayConverter(fieldTypes, fields, false));
			}
		};
	}
}
