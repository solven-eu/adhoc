/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.atoti;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.cellset.ICellSet;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.query.IGetAggregatesQuery;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.query.IQuery;
import com.quartetfs.fwk.query.QueryException;

import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.IRowsStream;
import eu.solven.adhoc.table.SuppliedRowsStream;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.IdentityImplicitTranscoder;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@Builder
public class AdhocAtotiTable implements IAdhocTableWrapper {
	@NonNull
	final IActivePivotManager apManager;

	@NonNull
	final String pivotId;

	@NonNull
	@Builder.Default
	@Getter
	final IAdhocTableTranscoder transcoder = new IdentityImplicitTranscoder();

	@Override
	public IRowsStream openDbStream(TableQuery tableQuery) {
		IActivePivotVersion ap = inferPivotId();

		String pivotId = ap.getId();
		Collection<ILocation> locations = null;
		Collection<String> measureSelections = null;
		List<? extends IContextValue> contextValues = null;

		Object[] args = { pivotId, locations, measureSelections, contextValues };

		IQuery<ICellSet> gaq = Registry.createExtendedPluginValue(IQuery.class, IGetAggregatesQuery.PLUGIN_KEY, args);

		ICellSet result;
		try {
			result = ap.execute(gaq);
		} catch (QueryException e) {
			throw new IllegalStateException("Issue executing %s".formatted(gaq), e);
		}

		// TODO Return as an Iterator/Stream
		List<Map<String, ?>> asList = new ArrayList<>();

		result.forEachLocation(location -> {
			asList.add(asMap(tableQuery, result, location));

			return true;
		});

		return new SuppliedRowsStream(tableQuery, asList::stream);
	}

	private Map<String, ?> asMap(TableQuery dbQuery, ICellSet result, int locationIndex) {
		Map<String, Object> map = new LinkedHashMap<>();

		dbQuery.getGroupBy().getGroupedByColumns().forEach(column -> {
			map.put(column, getColumnCoordinate(dbQuery, result, locationIndex, column));
		});

		return map;
	}

	private Object getColumnCoordinate(TableQuery dbQuery, ICellSet result, int locationIndex, String column) {
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

	private IActivePivotVersion inferPivotId() {
		IMultiVersionActivePivot mvActivePivot = MapPathGet.getRequiredAs(apManager.getActivePivots(), pivotId);
		return mvActivePivot.getHead();
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		Map<String, Class<?>> columns = new HashMap<>();

		inferPivotId().getDimensions().forEach(d -> {
			d.getHierarchies().forEach(h -> {
				h.getLevels().forEach(l -> {
					columns.put("%s@%s@%s".formatted(l.getName(), h.getName(), d.getName()), Object.class);
				});
			});
		});

		return columns;
	}

	@Override
	public String getName() {
		return pivotId;
	}
}
