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
package eu.solven.adhoc.table.transcoder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import eu.solven.adhoc.column.CalculatedColumn;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Given an {@link ITableTranscoder}, this will remember how each column has been transcoded. It serves multiple
 * purposes. The first one is not to require {@link ITableTranscoder} to express a bi-directional mapping. The second
 * main purpose is the reverse mapping may be ambiguous, when multiple queries columns could be served by the same
 * underlying column.
 */
@Builder
@Slf4j
public class TranscodingContext implements ITableTranscoder, IAdhocTableReverseTranscoder {
	final SetMultimap<String, String> underlyingToQueried = MultimapBuilder.hashKeys().hashSetValues().build();

	@Getter
	final Map<String, CalculatedColumn> nameToCalculated = new LinkedHashMap<>();

	@Builder.Default
	final ITableTranscoder transcoder = new IdentityImplicitTranscoder();

	@Override
	public String underlying(String queried) {
		String underlyingColumn = transcoder.underlyingNonNull(queried);

		if (underlyingToQueried.put(underlyingColumn, queried)) {
			log.trace("Registered {} -> {} in {}", underlyingColumn, queried, this);
		}

		return underlyingColumn;
	}

	@Override
	public Set<String> queried(String underlying) {
		return underlyingToQueried.get(underlying);
	}

	public Set<String> underlyings() {
		return underlyingToQueried.keySet();
	}

	public void addCalculatedColumn(CalculatedColumn calculatedColumn) {
		nameToCalculated.put(calculatedColumn.getName(), calculatedColumn);
	}

	@Override
	public int estimateSize(Set<String> underlyingKeys) {
		long asLong = underlyingKeys.stream().mapToLong(k -> underlyingToQueried.get(k).size()).sum();
		return Ints.checkedCast(asLong);
	}

}
