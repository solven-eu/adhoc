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
package eu.solven.adhoc.table.sql;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.dataframe.row.TabularRecordBuilder;
import eu.solven.adhoc.map.factory.ColumnSliceFactory;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Tests for {@link JooqTabularRecordFactory}.
 *
 * @author Benoit Lacelle
 */
public class TestJooqTabularRecordFactory {

	protected ISliceFactory sliceFactory =
			ColumnSliceFactory.builder().options(IHasOptionsAndExecutorService.noOption()).build();

	@Test
	public void testMakeRecord_singleAggregate() {
		ITabularRecordFactory factory = makeFactory(List.of("sumV"), ImmutableSet.of("k1"));

		Record r = DSL.using(org.jooq.SQLDialect.DEFAULT)
				.newRecord(DSL.field("sumV", SQLDataType.BIGINT), DSL.field("k1", SQLDataType.VARCHAR));
		r.set(DSL.field("sumV", SQLDataType.BIGINT), 42L);
		r.set(DSL.field("k1", SQLDataType.VARCHAR), "A");

		ITabularRecord record = JooqTabularRecordFactory.makeRecord(factory, r);

		Assertions.assertThat((Map) record.asMap()).containsEntry("k1", "A");
		Assertions.assertThat(record.onAggregate("sumV")).isNotNull();
	}

	@Test
	public void testMakeRecord_duplicateAggregateColumnName() {
		// Two aggregates with the same column name — ImmutableMap.Builder.buildOrThrow will reject this
		ITabularRecordFactory factory = makeFactory(List.of("sumV", "sumV"), ImmutableSet.of("k1"));

		Record r = DSL.using(org.jooq.SQLDialect.DEFAULT)
				.newRecord(DSL.field("sumV1", SQLDataType.BIGINT),
						DSL.field("sumV2", SQLDataType.BIGINT),
						DSL.field("k1", SQLDataType.VARCHAR));
		r.set(DSL.field("sumV1", SQLDataType.BIGINT), 10L);
		r.set(DSL.field("sumV2", SQLDataType.BIGINT), 20L);
		r.set(DSL.field("k1", SQLDataType.VARCHAR), "A");

		Assertions.assertThatThrownBy(() -> JooqTabularRecordFactory.makeRecord(factory, r))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Multiple entries with same key: sumV=20 and sumV=10");
	}

	@Test
	public void testMakeRecord_nullAggregate() {
		ITabularRecordFactory factory = makeFactory(List.of("sumV"), ImmutableSet.of("k1"));

		Record r = DSL.using(org.jooq.SQLDialect.DEFAULT)
				.newRecord(DSL.field("sumV", SQLDataType.BIGINT), DSL.field("k1", SQLDataType.VARCHAR));
		// sumV is null (not set)
		r.set(DSL.field("k1", SQLDataType.VARCHAR), "B");

		ITabularRecord record = JooqTabularRecordFactory.makeRecord(factory, r);

		Assertions.assertThat((Map) record.asMap()).containsEntry("k1", "B");
	}

	/**
	 * Builds a simple {@link ITabularRecordFactory} for testing.
	 */
	protected ITabularRecordFactory makeFactory(List<String> aggregates, ImmutableSet<String> columns) {
		IGroupBy groupBy = GroupByColumns.named(columns);

		return new ITabularRecordFactory() {
			@Override
			public List<String> getAggregates() {
				return aggregates;
			}

			@Override
			public ImmutableSet<String> getColumns() {
				return columns;
			}

			@Override
			public List<String> getOptionalColumns() {
				return List.of();
			}

			@Override
			public TabularRecordBuilder makeTabularRecordBuilder(Set<String> absentColumns) {
				return new TabularRecordBuilder(groupBy,
						ImmutableMap.builderWithExpectedSize(aggregates.size()),
						sliceFactory.newMapBuilder(Sets.difference(columns, absentColumns)));
			}
		};
	}
}
