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
package eu.solven.adhoc.dataframe.row;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.tabular.ITabularGroupByRecord;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.query.cube.IGroupBy;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.With;

/**
 * A simple {@link ITabularRecord} based on {@link Map}.
 *
 * @author Benoit Lacelle
 */
@Builder(toBuilder = true)
public class TabularGroupByRecordOverMap implements ITabularGroupByRecord {
	@NonNull
	@With
	@Getter
	final IGroupBy groupBy;

	@NonNull
	@With
	final ISlice slice;

	@Override
	public ISlice asSlice() {
		return slice;
	}

	@Override
	public Set<String> columnsKeySet() {
		return slice.asAdhocMap().keySet();
	}

	@Override
	public Object getGroupBy(String column) {
		IAdhocMap asMap = slice.asAdhocMap();

		Object value = asMap.get(column);
		if (value == null && !asMap.containsKey(column)) {
			throw new IllegalArgumentException(
					"%s is not a sliced column, amongst %s".formatted(column, columnsKeySet()));
		}
		return value;
	}

	@Override
	public Optional<Object> optGroupBy(String column) {
		return Optional.ofNullable(slice.asAdhocMap().get(column));
	}

	@Override
	public String toString() {
		return toString(this);
	}

	@SuppressWarnings({ "PMD.ConsecutiveAppendsShouldReuse" })
	public static String toString(ITabularGroupByRecord tabularRecord) {
		StringBuilder string = new StringBuilder();

		string.append("slice:{");
		string.append(tabularRecord.columnsKeySet()
				.stream()
				.map(column -> column + "=" + tabularRecord.getGroupBy(column))
				.collect(Collectors.joining(", ")));
		string.append('}');

		return string.toString();
	}

	@Override
	public void forEachGroupBy(BiConsumer<? super String, ? super Object> action) {
		slice.asAdhocMap().forEach(action);
	}

	@Override
	public ITabularGroupByRecord retainAll(NavigableSet<String> columns) {
		return toBuilder().groupBy(groupBy.retainAll(columns)).slice(slice.retainAll(columns)).build();
	}
}
