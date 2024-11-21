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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import eu.solven.adhoc.execute.FilterHelpers;
import eu.solven.adhoc.query.DatabaseQuery;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

@Builder
public class InMemoryDatabase implements IAdhocDatabaseWrapper {

	@Default
	@NonNull
	@Getter
	final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

	@NonNull
	@Default
	List<Map<String, ?>> rows = new ArrayList<>();

	public void add(Map<String, ?> row) {
		rows.add(row);
	}

	protected Stream<Map<String, ?>> stream() {
		return rows.stream();
	}

	protected Map<String, ?> transcode(Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcoder, underlyingMap);
	}

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
		return this.stream().filter(row -> {
			return FilterHelpers.match(transcoder, dbQuery.getFilter(), row);
		}).map(row -> {
			// BEWARE Should we filter not selected columns?
			Set<String> keysToKeep = new HashSet<>();

			keysToKeep.addAll(dbQuery.getGroupBy().getGroupedByColumns());

			dbQuery.getAggregators().stream().map(a -> a.getColumnName()).forEach(keysToKeep::add);

			return row;
		}).map(this::transcode);
	}
}
