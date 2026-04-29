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
package eu.solven.adhoc.measure.decomposition.join;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.column.ColumnMetadata;

/**
 * Simple in-memory {@link IJoinDefinition} for tests. Each entry maps a composite key to a row of output columns.
 */
public class InMemoryJoinDefinition implements IJoinDefinition {
	private final Map<Map<String, Object>, Map<String, Object>> data = new LinkedHashMap<>();

	/**
	 * @param key
	 *            composite input-column key.
	 * @param outputs
	 *            map of output-column name → value.
	 */
	public void put(Map<String, Object> key, Map<String, Object> outputs) {
		data.put(key, outputs);
	}

	@Override
	public Optional<Map<String, Object>> lookup(Map<String, Object> joinKey) {
		return Optional.ofNullable(data.get(joinKey));
	}

	@Override
	public Set<Object> allOutputValues(String outputColumn) {
		return data.values()
				.stream()
				.map(row -> row.get(outputColumn))
				.filter(v -> v != null)
				.collect(Collectors.toSet());
	}

	@Override
	public Collection<ColumnMetadata> getColumns() {
		SetMultimap<String, ColumnMetadata> columnToMetadata =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		data.values().forEach(joined -> {
			joined.forEach((column, value) -> {
				if (value != null) {
					columnToMetadata.put(column,
							ColumnMetadata.builder().name(column).type(value.getClass()).tag("joined").build());
				}
			});
		});

		// Delegate class unification to `ColumnMetadata.merge`, which computes the common ancestor of observed types
		// rather than picking an arbitrary one.
		return Multimaps.asMap(columnToMetadata).values().stream().map(ColumnMetadata::merge).toList();
	}
}
