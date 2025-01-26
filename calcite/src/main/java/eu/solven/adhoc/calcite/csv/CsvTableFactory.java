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

import java.io.File;
import java.util.Map;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Factory that creates a {@link CsvTranslatableTable}.
 *
 * <p>
 * Allows a CSV table to be included in a model.json file, even in a schema that is not based upon {@link AdhocSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvTableFactory implements TableFactory<CsvTable> {
	// public constructor, per factory contract
	public CsvTableFactory() {
	}

	@Override
	public CsvTable create(SchemaPlus schema, String name, Map<String, Object> operand, @Nullable RelDataType rowType) {
		String fileName = (String) operand.get("file");
		final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
		final Source source = Sources.file(base, fileName);
		RelProtoDataType protoRowType;
		if (rowType != null) {
			protoRowType = RelDataTypeImpl.proto(rowType);
		} else {
			protoRowType = null;
		}
		return new CsvScannableTable(source, protoRowType);
	}
}
