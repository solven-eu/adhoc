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

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import eu.solven.adhoc.beta.schema.AdhocSchema;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class AdhocCalciteSchema extends AbstractSchema {

	final AdhocSchema schema;

	@Override
	public boolean isMutable() {
		// Adhoc enables only queries to its own underlying database
		return false;
	}

	/**
	 * Creates a CSV schema.
	 *
	 * @param aqe
	 */
	public AdhocCalciteSchema(AdhocSchema schema) {
		this.schema = schema;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		return schema.getNameToCube()
				.entrySet()
				.stream()
				.collect(Collectors.toMap(e -> e.getKey(), e -> new AdhocCalciteTable(e.getValue())));
	}
}
