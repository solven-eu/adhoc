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
package eu.solven.adhoc.dag;

import java.util.Set;

import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.database.IAdhocTableWrapper;
import eu.solven.adhoc.query.IQueryOption;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Combines an {@link AdhocQueryEngine}, including its {@link AdhocMeasureBag} and a {@link IAdhocTableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocCubeWrapper implements IAdhocCubeWrapper {
	@NonNull
	final IAdhocQueryEngine engine;
	@NonNull
	final IAdhocMeasureBag measures;
	@NonNull
	final IAdhocTableWrapper table;

	@Override
	public ITabularView execute(IAdhocQuery query, Set<? extends IQueryOption> options) {
		return engine.execute(query, options, measures, table);
	}

	/**
	 * Typically useful when the {@link IAdhocDatabaseWrapper} has to be changed, but the other parameters must be kept.
	 * 
	 * @param template
	 *            some template
	 * @return
	 */
	public static AdhocCubeWrapperBuilder edit(AdhocCubeWrapper template) {
		return AdhocCubeWrapper.builder().engine(template.engine).measures(template.measures).table(template.table);
	}

	@Override
	public Set<String> getColumns() {
		return table.getColumns();
	}
}
