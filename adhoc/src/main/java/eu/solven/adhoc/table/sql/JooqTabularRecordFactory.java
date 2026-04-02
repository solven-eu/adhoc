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
import org.jooq.exception.InvalidResultException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.dataframe.row.TabularRecordBuilder;
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

	public static ITabularRecord makeRecord(ITabularRecordFactory tabularRecordFactory, Record r) {

		Set<String> absentColumns = tabularRecordFactory.getOptionalColumns().stream().filter(c -> {
			Field<?> groupingField = r.field(JooqTableQueryFactory.groupingAlias(c));

			return !Integer.valueOf(0).equals(groupingField.getValue(r));
		}).collect(ImmutableSet.toImmutableSet());

		TabularRecordBuilder recordBuilder = tabularRecordFactory.makeTabularRecordBuilder(absentColumns);

		int columnShift = 0;

		List<String> aggregateFields = tabularRecordFactory.getAggregates();
		{
			int size = aggregateFields.size();

			for (int i = 0; i < size; i++) {
				Object value = r.get(columnShift + i);
				if (value != null) {
					String columnName = aggregateFields.get(i);
					Object previousValue = recordBuilder.appendAggregate(columnName, value);

					if (previousValue != null) {
						throw new InvalidResultException("Field " + columnName + " is not unique in Record : " + r);
					}
				}
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

}
