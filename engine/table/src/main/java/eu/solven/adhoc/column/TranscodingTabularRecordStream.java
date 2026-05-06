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
package eu.solven.adhoc.column;

import java.util.Set;
import java.util.function.Consumer;

import eu.solven.adhoc.dataframe.filter.FilterMatcher;
import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.ITableReverseAliaser;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import lombok.RequiredArgsConstructor;

/**
 * Applies {@link ColumnsManager}'s post-table pipeline (type transcoding, column-name reverse-aliasing, calculated
 * column evaluation, post-filter, projection) to the raw stream returned by the underlying table.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class TranscodingTabularRecordStream implements ITabularRecordStream {
	protected final ColumnsManager columnsManager;
	protected final AliasingContext transcodingContext;
	protected final ITabularRecordStream delegate;
	protected final ISliceFilter postFilter;
	protected final TranscodedResult transcodedQuery;

	@Override
	public boolean isDistinctSlices() {
		// TODO Study how this flag could be impacted by transcoding
		if (transcodingContext.getNameToCalculated().isEmpty()) {
			return delegate.isDistinctSlices();
		} else {
			// TODO Investigate deeper this case
			// But a calculated column could lead to additional groupBys. Hence, we may receive multiple entries
			// for a slice given columns of the original query
			return false;
		}
	}

	@Override
	public IConsumingStream<ITabularRecord> records() {
		// Push-based implementation that delegates to forEach, which is
		// concurrent-safe (e.g. Arrow CONCURRENT batches).
		return ConsumingStream.<ITabularRecord>builder().source(this::forEach).build();
	}

	protected void forEach(Consumer<ITabularRecord> consumer) {
		IColumnValueTranscoder valueTranscoder = columnsManager.prepareTypeTranscoder(transcodingContext);
		ITableReverseAliaser columnTranscoder = columnsManager.prepareColumnTranscoder(transcodingContext);
		FilterMatcher postFilterer =
				FilterMatcher.builder().filter(postFilter).onMissingColumn(FilterMatcher.failOnMissing()).build();

		// Calculated columns added by `evaluateCalculated` — present in records' slice keysets after the
		// post-filter step, but not necessarily desired downstream depending on whether the user query
		// also groupBy'd by them.
		Set<String> calculatedColumnNames = transcodingContext.getNameToCalculated().keySet();

		delegate.records()
				.map(rawRecord -> columnsManager.transcodeTypes(valueTranscoder, rawRecord))
				// TODO Should we transcode type before or after columnNames?
				.map(typeTranscoded -> typeTranscoded.transcode(columnTranscoder))
				// calculate columns after transcoding, as these expression are generally table-independent
				.map(valueTranscoded -> columnsManager.evaluateCalculated(transcodingContext, valueTranscoded))
				.filter(withCalculated -> columnsManager.filterCalculatedColumns(postFilterer, withCalculated))
				// Project to the original groupBy keyset so `TabularRecordStreamReducer.columnsToMarker` matches
				// exactly: keeps user-groupBy'd calculated columns, drops underlyings hoisted by `transcodeQuery`.
				.map(withCalculated -> transcodedQuery.project(withCalculated, calculatedColumnNames))
				.forEach(consumer);
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public String toString() {
		return "Transcoding: " + delegate;
	}
}
