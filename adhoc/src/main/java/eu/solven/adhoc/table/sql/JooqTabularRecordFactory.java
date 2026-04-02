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
