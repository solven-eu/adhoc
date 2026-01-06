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
package eu.solven.adhoc.dictionary.page;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.dictionary.IAppendableColumnFactory;
import eu.solven.adhoc.map.factory.SequencedSetLikeList;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link IAppendableTable}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class FlexibleAppendableTable implements IAppendableTable {

	protected final AtomicReference<IAppendableTablePage> refFlexiblePage = new AtomicReference<>();
	protected final ConcurrentMap<List<String>, IAppendableTablePage> keyToPage = new ConcurrentHashMap<>();

	@Default
	@NonNull
	final IAppendableColumnFactory columnsFactory = new ObjectArrayColumnsFactory();

	@Default
	protected final int capacity = AdhocUnsafe.getPageSize();

	protected IAppendableTablePage makePage() {
		return AppendableTablePage.builder().capacity(capacity).columnsFactory(columnsFactory).build();
	}

	@Override
	public ITableRow nextRow() {
		while (true) {
			IAppendableTablePage currentPage = refFlexiblePage.get();
			if (currentPage == null) {
				nextPage(currentPage);
			} else {
				ITableRow nextRow = currentPage.pollNextRow();
				if (nextRow == null) {
					nextPage(currentPage);
				} else {
					return nextRow;
				}
			}
		}
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected void nextPage(IAppendableTablePage currentPage) {
		// the page is full
		IAppendableTablePage newCandidate = makePage();
		IAppendableTablePage witnessPage = refFlexiblePage.compareAndExchange(currentPage, newCandidate);
		if (witnessPage == currentPage) {
			// This thread has actually updated the page
			// newCandidate.allocate();
			log.trace("New page");
		}
	}

	@Override
	public ITableRow nextRow(SequencedSetLikeList keysLikeList) {
		while (true) {
			List<String> keysAsList = keysLikeList.asList();
			IAppendableTablePage currentPage = keyToPage.computeIfAbsent(keysAsList, k -> makePage());
			ITableRow nextRow = currentPage.pollNextRow();
			if (nextRow == null) {
				IAppendableTablePage newCandidate = makePage();
				if (keyToPage.replace(keysAsList, currentPage, newCandidate)) {
					log.trace("New page");
				}
			} else {
				return nextRow;
			}
		}
	}
}
