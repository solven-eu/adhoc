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
package eu.solven.adhoc.data.row;

import java.util.Map;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.map.StandardSliceFactory.MapBuilderPreKeys;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import lombok.RequiredArgsConstructor;

/**
 * Helps creating an {@link ITabularRecord}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public class TabularRecordBuilder {
	final Map<String, Object> aggregates;
	final MapBuilderPreKeys sliceBuilder;

	protected Object cleanAggregateValue(Object value) {
		// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
		return AdhocPrimitiveHelpers.normalizeValue(value);
	}

	public Object appendAggregate(String columnName, Object value) {
		return aggregates.put(columnName, cleanAggregateValue(value));
	}

	public void appendGroupBy(@Nullable Object coordinate) {
		sliceBuilder.append(coordinate);
	}

	public ITabularRecord build() {
		return TabularRecordOverMaps.builder().aggregates(aggregates).slice(sliceBuilder.build().asSlice()).build();
	}

}
