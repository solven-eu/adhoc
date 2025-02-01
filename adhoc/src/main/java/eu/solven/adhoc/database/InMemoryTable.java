/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.database.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.database.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.database.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.database.transcoder.IdentityTranscoder;
import eu.solven.adhoc.database.transcoder.TranscodingContext;
import eu.solven.adhoc.execute.FilterHelpers;
import eu.solven.adhoc.query.TableQuery;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple {@link IAdhocTableWrapper} over a {@link List} of {@link Map}. It has some specificities: it does not
 * execute groupBys, nor it handles calculated columns (over SQL expressions).
 */
@Slf4j
@Builder
public class InMemoryTable implements IAdhocTableWrapper {

	public static InMemoryTable newInstance(Map<String, ?> options) {
		return InMemoryTable.builder().build();
	}

	@Default
	@NonNull
	@Getter
	String name = "inMemory";

	@Default
	@NonNull
	@Getter
	final IAdhocTableTranscoder transcoder = new IdentityTranscoder();

	@NonNull
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

	public void add(Map<String, ?> row) {
		rows.add(row);
	}

	protected Stream<Map<String, ?>> stream() {
		return rows.stream();
	}

	protected Map<String, ?> transcodeFromDb(IAdhocTableReverseTranscoder transcodingContext,
			Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcodingContext, underlyingMap);
	}

	@Override
	public IRowsStream openDbStream(TableQuery dbQuery) {
		TranscodingContext transcodingContext = TranscodingContext.builder().transcoder(transcoder).build();

		Set<String> queriedColumns = new HashSet<>();
		queriedColumns.addAll(dbQuery.getGroupBy().getGroupedByColumns());
		dbQuery.getAggregators().stream().map(a -> a.getColumnName()).forEach(queriedColumns::add);

		Set<String> underlyingColumns = new HashSet<>();
		queriedColumns.forEach(keyToKeep -> {
			// We need to call `transcodingContext.underlying` to register the reverse mapping
			String underlying = transcodingContext.underlying(keyToKeep);
			log.debug("{} -> {}", keyToKeep, underlying);
			underlyingColumns.add(underlying);
		});

		return new SuppliedRowsStream(() -> this.stream().filter(row -> {
			return FilterHelpers.match(transcodingContext, dbQuery.getFilter(), row);
		}).map(row -> {
			Map<String, Object> withSelectedColumns = new LinkedHashMap<>(row);

			// Discard the column not expressed by the dbQuery
			withSelectedColumns.keySet().retainAll(underlyingColumns);

			return withSelectedColumns;
		}).map(row -> transcodeFromDb(transcodingContext, row)));
	}

	@Override
	public Set<String> getColumns() {
		return rows.stream().flatMap(row -> row.keySet().stream()).collect(Collectors.toSet());
	}

}
