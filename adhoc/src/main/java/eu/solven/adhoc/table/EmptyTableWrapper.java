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
package eu.solven.adhoc.table;

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.SuppliedTabularRecordStream;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * {@link ITableWrapper} which is always empty. Useful when the {@link IAdhocDatabaseWrapper} to use is not yet known
 * (e.g. when one has to switch the underlying table depending on some queried filter).
 *
 * @author Benoit Lacelle
 */
@Builder
public class EmptyTableWrapper implements ITableWrapper {

	@NonNull
	@Getter
	final String name;

	@Override
	public ITabularRecordStream streamSlices(ExecutingQueryContext executingQueryContext, TableQuery tableQuery) {
		return new SuppliedTabularRecordStream("empty", Stream::empty);
	}

	@Override
	public Map<String, Class<?>> getColumns() {
		return Map.of();
	}
}
