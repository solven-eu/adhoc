/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.encoding.column.ObjectArrayColumnsFactory;
import eu.solven.adhoc.encoding.column.freezer.AdhocFreezingUnsafe;
import eu.solven.adhoc.encoding.column.freezer.AsynchronousFreezingStrategy;
import eu.solven.adhoc.encoding.column.freezer.IFreezingStrategy;
import eu.solven.adhoc.encoding.column.freezer.SynchronousFreezingStrategy;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.options.StandardQueryOptions;

/**
 * An {@link IAppendableTableFactory} based on {@link ScopedValueAppendableTable}. Opt-in alternative to
 * {@link ThreadLocalAppendableTableFactory} that targets the JDK-25 {@code ScopedValue} fast path.
 *
 * Callers MUST establish per-thread scopes around slice-factory usage — see {@link ScopedValueAppendableTable} Javadoc.
 * The engine wires this via {@link eu.solven.adhoc.map.factory.ISliceFactory#callWithScope} at each virtual-thread
 * dispatch site.
 *
 * @author Benoit Lacelle
 */
public class ScopedValueAppendableTableFactory implements IAppendableTableFactory {
	@Override
	public IAppendableTable makeTable(IHasOptionsAndExecutorService options) {
		IFreezingStrategy synchronousFreezer =
				SynchronousFreezingStrategy.builder().freezersWithContext(AdhocFreezingUnsafe.getFreezers()).build();

		IFreezingStrategy freezer;
		if (StandardQueryOptions.CONCURRENT.isActive(options.getOptions())) {
			freezer = AsynchronousFreezingStrategy.builder()
					.synchronousStrategy(synchronousFreezer)
					.executor(options.getExecutorService())
					.build();
		} else {
			freezer = synchronousFreezer;
		}

		return ScopedValueAppendableTable.builder()
				.capacity(AdhocColumnUnsafe.getPageSize())
				.columnsFactory(ObjectArrayColumnsFactory.builder().build())
				.freezer(freezer)
				.build();
	}
}
