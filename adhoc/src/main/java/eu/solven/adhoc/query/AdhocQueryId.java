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
package eu.solven.adhoc.query;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.adhoc.query.cube.IAdhocQuery;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * An {@link AdhocQueryId} is useful to atatch properly logs to a given query, especially in logs and eventBus events.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class AdhocQueryId {
	private static AtomicLong NEXT_QUERY_INDEX = new AtomicLong();

	@Default
	long queryIndex = NEXT_QUERY_INDEX.getAndIncrement();

	// queryId is defaulted to a random UUID, with very low collision probability
	@Default
	UUID queryId = UUID.randomUUID();

	// a query may have a parent, for instance in a CompositeCube, the compositeQuery would be the parent of the
	// underlyingQuerys
	@Default
	UUID parentQueryId = null;

	// Some queryHash to help as a human to see/search given query is re-used or edited
	@Default
	String queryHash = "";

	public static AdhocQueryId from(IAdhocQuery query) {
		return AdhocQueryId.builder().queryHash(Integer.toHexString(query.toString().hashCode())).build();
	}
}
