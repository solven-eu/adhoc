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
package eu.solven.adhoc.compression.page;

import java.util.List;

import eu.solven.adhoc.compression.column.IAppendableColumnFactory;
import eu.solven.adhoc.compression.column.ObjectArrayColumnsFactory;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.util.AdhocUnsafe;
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
	final IAppendableColumnFactory columnsFactory = ObjectArrayColumnsFactory.builder().build();

	@Default
	protected final int capacity = AdhocUnsafe.getPageSize();

	protected IAppendableTablePage makePage() {
		return AppendableTablePage.builder().capacity(capacity).columnsFactory(columnsFactory).build();
	}

	@Override
	public ITableRowWrite nextRow() {
		while (true) {
			IAppendableTablePage currentPage = getCurrentPage();
			if (currentPage == null) {
				nextPage(currentPage);
			} else {
				ITableRowWrite nextRow = currentPage.pollNextRow();
				if (nextRow == null) {
					nextPage(currentPage);
				} else {
					return nextRow;
				}
			}
		}
	}

	protected abstract IAppendableTablePage getCurrentPage();

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected void nextPage(IAppendableTablePage currentPage) {
		// the page is full
		IAppendableTablePage newCandidate = makePage();
		IAppendableTablePage witnessPage = compareAndSetPage(currentPage, newCandidate);
		if (witnessPage == currentPage) {
			// This thread has actually updated the page
			// newCandidate.allocate();
			log.trace("New page");
		}
	}

	protected abstract IAppendableTablePage compareAndSetPage(IAppendableTablePage currentPage,
			IAppendableTablePage newCandidate);

	@Override
	public ITableRowWrite nextRow(SequencedSetLikeList keysLikeList) {
		while (true) {
			List<String> keysAsList = keysLikeList.asList();
			IAppendableTablePage currentPage = getCurrentPage(keysAsList);
			ITableRowWrite nextRow = currentPage.pollNextRow();
			if (nextRow == null) {
				IAppendableTablePage newCandidate = makePage();
				if (compareAndSetPage(keysAsList, currentPage, newCandidate)) {
					log.trace("New page");
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
