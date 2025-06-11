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
package eu.solven.adhoc.atoti.table;

import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.query.IQueryable;

import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Wraps an {@link IActivePivotManager} and rely on JooQ to use it as database for {@link TableQuery}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
public class AdhocAtotiTable extends AAdhocAtotiTable {
	@NonNull
	final IActivePivotManager apManager;

	@NonNull
	@Getter
	final String pivotId;

	@Override
	protected IActivePivotVersion inferPivotId() {
		IMultiVersionActivePivot mvActivePivot = MapPathGet.getRequiredAs(apManager.getActivePivots(), pivotId);
		return mvActivePivot.getHead();
	}

	@Override
	protected IQueryable inferQueryable() {
		return inferPivotId();
	}
}
