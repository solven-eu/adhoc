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
package eu.solven.adhoc.engine.tabular;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.options.StandardQueryOptions;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * Logs while iterating through a {@link ITabularRecordStream}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class TabularRecordLogger {
	final AtomicInteger nbIn = new AtomicInteger();

	final String table;

	@Singular
	final Set<IQueryOption> options;

	public Runnable closeHandler() {
		return () -> {
			if (StandardQueryOptions.EXPLAIN.isActive(options)) {
				log.info("[EXPLAIN] Aggregates from table completed accepting {} rows (table={})", nbIn.get(), table);
			} else {
				log.debug("[DEBUG] Aggregates from table completed accepting {} rows (table={})", nbIn.get(), table);
			}
		};
	}

	public BiConsumer<ITabularRecord, IAdhocSlice> prepareStreamLogger(IHasQueryOptions tableQuery) {
		return (input, optCoordinates) -> {
			int currentIn = nbIn.incrementAndGet();
			if (logAboutRow(tableQuery, currentIn)) {
				log.info("[DEBUG] Accepted row #{}: {} (table={})", currentIn, input, table);
			}
		};
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected boolean logAboutRow(IHasQueryOptions tableQuery, int currentOut) {
		return tableQuery.isDebug() && (currentOut <= 16 || Integer.bitCount(currentOut) == 1);
	}

}
