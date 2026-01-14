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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link IAppendableTable}.
 * 
 * It is thread-safe by creating an {@link IAppendableTablePage} per calling thread. Given page should then written in a
 * monothreaded way.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@ThreadSafe
public class ThreadLocalAppendableTable extends AAppendableTable {
	// This could be replaced by an AtomicReference if the IAppendableTablePage implementation was thread-safe.
	protected final ThreadLocal<IAppendableTablePage> refFlexiblePage = new ThreadLocal<>();
	protected final ThreadLocal<Map<List<String>, IAppendableTablePage>> keyToPage = new ThreadLocal<>() {
		@Override
		protected Map<List<String>, IAppendableTablePage> initialValue() {
			return new HashMap<>();
		}
	};

	@Override
	protected IAppendableTablePage getCurrentPage() {
		return refFlexiblePage.get();
	}

	@Override
	protected IAppendableTablePage compareAndSetPage(IAppendableTablePage currentPage,
			IAppendableTablePage newCandidate) {
		refFlexiblePage.set(newCandidate);

		return newCandidate;
	}

	@Override
	protected IAppendableTablePage getCurrentPage(List<String> keysAsList) {
		return keyToPage.get().computeIfAbsent(keysAsList, k -> makePage());
	}

	@Override
	protected boolean compareAndSetPage(List<String> keysAsList,
			IAppendableTablePage currentPage,
			IAppendableTablePage newCandidate) {
		return keyToPage.get().replace(keysAsList, currentPage, newCandidate);
	}
}
