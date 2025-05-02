package eu.solven.adhoc.table.cache;

import java.util.Collection;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;

/**
 * A decorating {@link ITableWrapper}
 * 
 * @author Benoit Lacelle
 */
public class CachingTableWrapper implements ITableWrapper {
	
	ITableWrapper decorated;

	@Override
	public Collection<ColumnMetadata> getColumns() {
		return decorated.getColumns();
	}

	@Override
	public String getName() {
		// Do not prefix/edit the name, as we typically want this ITableWrapper to work in name of the underlying table
		return decorated.getName();
	}

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQueryV2 tableQuery) {
		return null;
	}

}
