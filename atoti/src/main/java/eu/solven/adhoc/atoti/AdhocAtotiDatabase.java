package eu.solven.adhoc.atoti;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.cellset.ICellSet;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.query.IGetAggregatesQuery;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.query.IQuery;
import com.quartetfs.fwk.query.QueryException;

import eu.solven.adhoc.database.IAdhocDatabaseWrapper;
import eu.solven.adhoc.query.DatabaseQuery;
import lombok.Builder;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link DatabaseQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
public class AdhocAtotiDatabase implements IAdhocDatabaseWrapper {
	final IActivePivotManager apManager;

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {

		String pivotId = null;
		Collection<ILocation> locations = null;
		Collection<String> measureSelections = null;
		List<? extends IContextValue> contextValues = null;

		Object[] args = { pivotId, locations, measureSelections, contextValues };

		IQuery<ICellSet> gaq = Registry.createExtendedPluginValue(IQuery.class, IGetAggregatesQuery.PLUGIN_KEY, args);

		IActivePivotVersion ap = apManager.getActivePivots().get(inferPivotId(dbQuery)).getHead();

		ICellSet result;
		try {
			result = ap.execute(gaq);
		} catch (QueryException e) {
			throw new IllegalStateException("Issue executing %s".formatted(gaq));
		}

		// TODO Return as an Iterator/Stream
		List<Map<String, ?>> asList = new ArrayList<>();

		result.forEachLocation(location -> {
			asList.add(asMap(dbQuery, result, location));

			return true;
		});

		return asList.stream();
	}

	private Map<String, ?> asMap(DatabaseQuery dbQuery, ICellSet result, int locationIndex) {
		Map<String, Object> map = new LinkedHashMap<>();

		dbQuery.getGroupBy().getGroupedByColumns().forEach(column -> {
			map.put(column, getColumnCoordinate(dbQuery, result, locationIndex, column));
		});

		return map;
	}

	private Object getColumnCoordinate(DatabaseQuery dbQuery, ICellSet result, int locationIndex, String column) {
		// result.getCoordinate(locationIndex, result., locationIndex)
		ILocation l = result.getLocation(locationIndex);

		return l.getCoordinate(getHierarchyIndex(column), getLevelIndex(column));
	}

	private int getLevelIndex(String column) {
		return 0;
	}

	private int getHierarchyIndex(String column) {
		return 0;
	}

	private String inferPivotId(DatabaseQuery dbQuery) {
		return apManager.getActivePivots().keySet().iterator().next();
	}
}
