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

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.table.TableQueryV2;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link ITableWrapper} which will returns result from a main {@link ITableWrapper}, but it will also send the
 * request to alternative {@link ITableWrapper}.
 *
 * It is useful for development purposes, e.g. to check the output from `EXPLAIN` for each fork.
 */
@Builder
@Slf4j
public class ForkingTableWrapper implements ITableWrapper {
	@NonNull
	ITableWrapper main;

	@NonNull
	@Singular
	ImmutableList<ITableWrapper> forks;

	@Override
	public ITabularRecordStream streamSlices(QueryPod queryPod, TableQueryV2 tableQuery) {
		ITabularRecordStream mainStream = this.main.streamSlices(queryPod, tableQuery);

		List<ITabularRecordStream> forkedStreams =
				forks.stream().map(fork -> fork.streamSlices(queryPod, tableQuery)).toList();

		return new ITabularRecordStream() {
			@Override
			public boolean isDistinctSlices() {
				return mainStream.isDistinctSlices();
			}

			@Override
			public Stream<ITabularRecord> records() {
				forkedStreams.forEach(forkedStream -> {
					List<ITabularRecord> forkedRecords = forkedStream.records().toList();
					log.info("Fork to {} returned {} rows", forkedStream, forkedRecords.size());
				});

				return mainStream.records();
			}

			@Override
			public void close() {
				mainStream.close();

				forkedStreams.forEach(ITabularRecordStream::close);
			}
		};
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		return List.of();
	}

	@Override
	public String getName() {
		return "";
	}
}
