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
package eu.solven.adhoc.encoding.page;

import java.util.List;

import eu.solven.adhoc.collection.ILikeList;
import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.encoding.column.IAppendableColumnFactory;
import eu.solven.adhoc.encoding.column.ObjectArrayColumnsFactory;
import eu.solven.adhoc.encoding.column.freezer.IFreezingStrategy;
import eu.solven.adhoc.encoding.column.freezer.SynchronousFreezingStrategy;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link IAppendableTable}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public abstract class AAppendableTable implements IAppendableTable {

	@Default
	@NonNull
	protected final IAppendableColumnFactory columnsFactory = ObjectArrayColumnsFactory.builder().build();

	@Default
	@NonNull
	protected IFreezingStrategy freezer = SynchronousFreezingStrategy.builder().build();

	@Default
	protected final int capacity = AdhocColumnUnsafe.getPageSize();

	protected IAppendableTablePage makePage() {
		return AppendableTablePage.builder().capacity(capacity).columnsFactory(columnsFactory).freezer(freezer).build();
	}

	@Override
	public ITableRowWrite nextRow(ILikeList<String> keysLikeList) {
		while (true) {
			List<String> keysAsList = keysLikeList.asList();
			IAppendableTablePage currentPage = getCurrentPage(keysAsList);
			ITableRowWrite nextRow = currentPage.pollNextRow();
			if (nextRow == null) {
				IAppendableTablePage newCandidate = makePage();
				if (compareAndSetPage(keysAsList, currentPage, newCandidate)) {
					log.trace("New page for keys={}", keysAsList);
				}
			} else {
				return nextRow;
			}
		}
	}

	protected abstract boolean compareAndSetPage(List<String> keysAsList,
			IAppendableTablePage currentPage,
			IAppendableTablePage newCandidate);

	protected abstract IAppendableTablePage getCurrentPage(List<String> keysAsList);
}
