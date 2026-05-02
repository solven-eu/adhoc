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
package eu.solven.adhoc.column.hierarchy;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Builder;
import lombok.Singular;

/**
 * A simple {@link IHierarchies} based on a {@link Map} from a hierarchy name to a {@link List} of columns.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class MapHierarchies implements IHierarchies {
	@Singular
	ImmutableMap<String, ImmutableList<String>> hierarchyToColumns;

	@Override
	public Optional<String> getParent(String column) {
		return hierarchyToColumns.entrySet().stream().filter(e -> e.getValue().contains(column)).map(e -> {
			int indexOf = e.getValue().indexOf(column);

			if (indexOf == 0) {
				return ROOT;
			} else {
				return e.getValue().get(indexOf - 1);
			}
		}).findFirst();
	}
}
