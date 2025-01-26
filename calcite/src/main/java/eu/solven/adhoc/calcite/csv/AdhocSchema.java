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

import java.util.Collections;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import eu.solven.adhoc.dag.IAdhocCubeWrapper;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class AdhocSchema extends AbstractSchema {

	final IAdhocCubeWrapper aqe;

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
	public AdhocSchema(IAdhocCubeWrapper aqe) {
		this.aqe = aqe;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		// if (tableMap == null) {
		// tableMap = createTableMap();
		// }
		// return tableMap;
		return Collections.singletonMap("zips", new MongoTable(aqe));
	}

	// private Map<String, Table> createTableMap() {
	// // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
	// // ".json.gz".
	// final Source baseSource = Sources.of(directoryFile);
	// File[] files = directoryFile.listFiles((dir, name) -> {
	// final String nameSansGz = trim(name, ".gz");
	// return nameSansGz.endsWith(".csv") || nameSansGz.endsWith(".json");
	// });
	// if (files == null) {
	// System.out.println("directory " + directoryFile + " not found");
	// files = new File[0];
	// }
	// // Build a map from table name to table; each file becomes a table.
	// final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
	// for (File file : files) {
	// Source source = Sources.of(file);
	// Source sourceSansGz = source.trim(".gz");
	// final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
	// if (sourceSansJson != null) {
	// final Table table = new JsonScannableTable(source);
	// builder.put(sourceSansJson.relative(baseSource).path(), table);
	// }
	// final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
	// if (sourceSansCsv != null) {
	// final Table table = createTable(source);
	// builder.put(sourceSansCsv.relative(baseSource).path(), table);
	// }
	// }
	// return builder.build();
	// }
	//
	// /** Creates different sub-type of table based on the "flavor" attribute. */
	// private Table createTable(Source source) {
	// switch (flavor) {
	// case TRANSLATABLE:
	// return new CsvTranslatableTable(source, null);
	// case SCANNABLE:
	// return new CsvScannableTable(source, null);
	// case FILTERABLE:
	// return new CsvFilterableTable(source, null);
	// default:
	// throw new AssertionError("Unknown flavor " + this.flavor);
	// }
	// }
}
