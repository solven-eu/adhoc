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
package eu.solven.adhoc.query.cube;

import java.util.Set;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Wraps an {@link IAdhocQuery} which is derived from a parent query.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public final class AdhocSubQuery implements IAdhocQuery, IHasParentQueryId {
	@NonNull
	final IAdhocQuery subQuery;
	@NonNull
	final AdhocQueryId parentQueryId;

	@Override
	public IAdhocFilter getFilter() {
		return subQuery.getFilter();
	}

	@Override
	public IAdhocGroupBy getGroupBy() {
		return subQuery.getGroupBy();
	}

	@Override
	public Set<IMeasure> getMeasures() {
		return subQuery.getMeasures();
	}

	@Override
	public Object getCustomMarker() {
		return subQuery.getCustomMarker();
	}

	@Override
	public boolean isExplain() {
		return subQuery.isExplain();
	}

	@Override
	public boolean isDebug() {
		return subQuery.isDebug();
	}

	@Override
	public Set<IQueryOption> getOptions() {
		return subQuery.getOptions();
	}

}
