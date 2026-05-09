/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.duckdb;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;

/**
 * End-to-end coverage of mixing {@link Aggregator#empty()} with a real measure on a DuckDB-backed cube. The contract:
 * when the user asks for {@code k1 + empty} grouped by {@code a}, every distinct {@code a} slice present in the table
 * must surface in the result — including slices where {@code k1} is missing (NULL in the source rows). The empty
 * aggregator's job is to materialize coordinates regardless of the real measure's per-slice contribution.
 *
 * @author Benoit Lacelle
 */
public class TestDagCubeQuery_DuckDb_emptyAggregator extends ATestDagDuckDb implements IAdhocTestConstants {

	String tableName = "someTableName";

	@Override
	public ITableWrapper makeTable() {
		return new JooqTableWrapper(tableName,
				DuckDBHelper.parametersBuilder(dslSupplier).tableName(tableName).build());
	}

	private void seedTable() {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.DOUBLE)
				.execute();

		// Two slices with a real k1 value
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", 123).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a2", 234).execute();

		// Two slices where k1 is missing (NULL) — without the empty aggregator, the SUM is NULL but the slice still
		// appears via standard GROUP BY semantics. The point of this test is that adding `empty` to the same query
		// continues to produce exactly the same slice set: the empty aggregator must not drop any coordinate, and
		// must not break the real aggregation alongside it.
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a3", null).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a4", null).execute();

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testQueryK1AndEmpty_groupByA_emitsSliceWhereK1IsMissing() {
		seedTable();

		Aggregator empty = Aggregator.empty();

		ITabularView result = cube().execute(CubeQuery.builder().measure(k1Sum, empty).groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// All four slices must appear, including the two where k1 is NULL. The slices with a real k1 value carry the
		// SUM; the others contribute only the empty aggregator's null marker, so the resulting per-slice map is empty
		// for them (no real-value entry under k1Sum's name).
		Assertions.assertThat(mapBased.getCoordinatesToValues().keySet())
				.containsExactlyInAnyOrder(Map.of("a", "a1"), Map.of("a", "a2"), Map.of("a", "a3"), Map.of("a", "a4"));

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("a", "a1"), Map.of(k1Sum.getName(), 123D))
				.containsEntry(Map.of("a", "a2"), Map.of(k1Sum.getName(), 234D));
	}

	// Sanity-check: the same query, restricted to the bare empty aggregator, still surfaces every slice.
	@Test
	public void testQueryEmptyOnly_groupByA_emitsAllSlices() {
		seedTable();

		Aggregator empty = Aggregator.empty();

		ITabularView result = cube().execute(CubeQuery.builder().measure(empty).groupByAlso("a").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues().keySet())
				.containsExactlyInAnyOrder(Map.of("a", "a1"), Map.of("a", "a2"), Map.of("a", "a3"), Map.of("a", "a4"));
	}
}
