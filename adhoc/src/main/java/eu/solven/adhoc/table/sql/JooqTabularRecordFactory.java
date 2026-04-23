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
import java.util.Set;

import org.jooq.Field;
import org.jooq.Record;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.dataframe.row.TabularRecordBuilder;
import eu.solven.adhoc.util.HotPath;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Turns JooQ {@link Record} into {@link ITabularRecord}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
public class JooqTabularRecordFactory {

	@HotPath("once per record for ITableWrapper")
	public static ITabularRecord makeRecord(ITabularRecordFactory tabularRecordFactory, Record r) {
		Set<String> absentColumns = getAbsentColumns(tabularRecordFactory, r);

		TabularRecordBuilder recordBuilder = tabularRecordFactory.makeTabularRecordBuilder(absentColumns);

		int columnShift = 0;

		List<String> aggregateFields = tabularRecordFactory.getAggregates();
		{
			int size = aggregateFields.size();

			for (int i = 0; i < size; i++) {
				Object value = r.get(columnShift + i);
				recordBuilder.appendAggregate(value);
			}

			columnShift += size;
		}

		{
			// Record fields may not match exactly the columns, especially on qualified fields
			ImmutableList<String> columns = tabularRecordFactory.getColumns().asList();
			int size = columns.size();

			int nbToAppend = size - absentColumns.size();
			int nbAppend = 0;

			if (absentColumns.size() != size) {
				for (int i = 0; i < size && nbAppend < nbToAppend; i++) {
					String currentKey = columns.get(i);
					if (absentColumns.contains(currentKey)) {
						log.debug("Skip NULL as {} not in current GROUPING SET", currentKey);
						continue;
					}

					recordBuilder.appendGroupBy(r.get(columnShift + i));
					nbAppend++;
				}
			}
		}

		return recordBuilder.build();
	}

	@HotPath("once per record for ITableWrapper")
	private static Set<String> getAbsentColumns(ITabularRecordFactory tabularRecordFactory, Record r) {
		if (tabularRecordFactory.getOptionalColumns().isEmpty()) {
			return ImmutableSet.of();
		} else {
			// TODO Could we cache this branch?
			ImmutableSet.Builder<String> absentColumns =
					ImmutableSet.builderWithExpectedSize(tabularRecordFactory.getOptionalColumns().size());

			tabularRecordFactory.getOptionalColumns().forEach(optionalColumn -> {
				Field<?> groupingField = r.field(JooqTableQueryFactory.groupingAlias(optionalColumn));

				if (!Integer.valueOf(0).equals(groupingField.getValue(r))) {
					absentColumns.add(optionalColumn);
				}
			});

			return absentColumns.build();
		}
	}

}
