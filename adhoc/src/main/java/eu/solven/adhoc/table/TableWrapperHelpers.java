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
package eu.solven.adhoc.table;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.stream.ConsumingStream;
import eu.solven.adhoc.stream.IConsumingStream;
import lombok.experimental.UtilityClass;

/**
 * Utility methods around {@link ITableWrapper}.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class TableWrapperHelpers {

	public static ITabularRecordStream v4ToV3(QueryPod queryPod,
			Stream<TableQueryV4> tableQuery,
			ITableWrapper tableWrapper) {
		List<ITabularRecordStream> underlyings =
				tableQuery.flatMap(TableQueryV4::streamV3).map(v3 -> tableWrapper.streamSlices(queryPod, v3)).toList();

		return composite(underlyings);
	}

	public static ITabularRecordStream v3TovV2(QueryPod queryPod,
			Stream<TableQueryV3> tableQuery,
			ITableWrapper tableWrapper) {
		List<ITabularRecordStream> underlyings =
				tableQuery.flatMap(TableQueryV3::streamV2).map(v2 -> tableWrapper.streamSlices(queryPod, v2)).toList();

		return composite(underlyings);
	}

	private static ITabularRecordStream composite(List<ITabularRecordStream> underlyings) {
		return new ITabularRecordStream() {

			@Override
			public IConsumingStream<ITabularRecord> records() {
				return ConsumingStream.<ITabularRecord>builder().source(consumer -> {
					underlyings.stream().forEach(s -> s.records().forEach(consumer));
				}).build();
			}

			@Override
			public boolean isDistinctSlices() {
				return false;
			}

			@Override
			public void close() {
				closeAll(underlyings);
			}
		};
	}

	// Relates with Guava Closer, but needed as Closed does not handle AutoCloseable
	// https://github.com/google/guava/issues/3068
	@SuppressWarnings("PMD.CloseResource")
	protected static void closeAll(List<ITabularRecordStream> streams) {
		Throwable firstThrown = null;
		for (ITabularRecordStream stream : streams) {
			try {
				stream.close();
			} catch (Exception e) {
				if (firstThrown == null) {
					firstThrown = e;
				} else {
					firstThrown.addSuppressed(e);
				}
			}
		}
		if (firstThrown instanceof RuntimeException re) {
			throw re;
		} else if (firstThrown != null) {
			throw new RuntimeException(firstThrown);
		}
	}

}
