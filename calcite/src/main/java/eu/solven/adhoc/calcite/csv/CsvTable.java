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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for table that reads CSV files.
 */
public abstract class CsvTable extends AbstractTable {
	protected final Source source;
	protected final @Nullable RelProtoDataType protoRowType;
	private @Nullable RelDataType rowType;
	private @Nullable List<RelDataType> fieldTypes;

	/** Creates a CsvTable. */
	CsvTable(Source source, @Nullable RelProtoDataType protoRowType) {
		this.source = source;
		this.protoRowType = protoRowType;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType != null) {
			return protoRowType.apply(typeFactory);
		}
		if (rowType == null) {
			rowType = CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, null, isStream());
		}
		return rowType;
	}

	/** Returns the field types of this CSV table. */
	public List<RelDataType> getFieldTypes(RelDataTypeFactory typeFactory) {
		if (fieldTypes == null) {
			fieldTypes = new ArrayList<>();
			CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, fieldTypes, isStream());
		}
		return fieldTypes;
	}

	/** Returns whether the table represents a stream. */
	protected boolean isStream() {
		return false;
	}

	/** Various degrees of table "intelligence". */
	public enum Flavor {
		SCANNABLE, FILTERABLE, TRANSLATABLE
	}
}
