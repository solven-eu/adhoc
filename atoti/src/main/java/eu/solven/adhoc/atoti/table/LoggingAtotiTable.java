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
package eu.solven.adhoc.atoti.table;

import java.util.Set;

import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.cellset.impl.EmptyCellSet;
import com.quartetfs.biz.pivot.query.IGetAggregatesQuery;
import com.quartetfs.biz.pivot.query.impl.GetAggregatesQuery;
import com.quartetfs.fwk.query.IQuery;
import com.quartetfs.fwk.query.IQueryable;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This simulates a ActivePivot/Atoti instance receiving {@link IGetAggregatesQuery}. Can be used to log about the
 * received {@link IGetAggregatesQuery}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class LoggingAtotiTable extends AAdhocAtotiTable implements IQueryable {

	@NonNull
	@Getter
	final String pivotId;

	@Override
	protected IActivePivotVersion inferPivotId() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected IQueryable inferQueryable() {
		return this;
	}

	@Override
	public Set<String> getSupportedQueries() {
		return Set.of(GetAggregatesQuery.PLUGIN_KEY);
	}

	@Override
	public <R> R execute(IQuery<R> query) {
		if (query instanceof IGetAggregatesQuery gaq) {
			log.info("pivot={} received gaq={}", getPivotId(), gaq);

			return (R) EmptyCellSet.getInstance();
		} else {
			throw new UnsupportedOperationException("query=%s".formatted(query));
		}
	}
}
