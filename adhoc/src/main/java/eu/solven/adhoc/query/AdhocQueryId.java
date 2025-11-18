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

import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IHasParentQueryId;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * An {@link AdhocQueryId} is useful to attach properly logs to a given query, especially in logs and eventBus events.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class AdhocQueryId {
	@Default
	long queryIndex = AdhocUnsafe.nextQueryIndex();

	// queryId is defaulted to a random UUID, with very low collision probability
	@NonNull
	@Default
	UUID queryId = AdhocUnsafe.randomUUID();

	// a query may have a parent, for instance in a CompositeCube, the compositeQuery would be the parent of the
	// underlyingQuerys
	@Default
	UUID parentQueryId = null;

	// Some queryHash to help as a human to see/search given query is re-used or edited
	@NonNull
	@Default
	String queryHash = "";

	@Default
	boolean cubeElseTable = true;

	@NonNull
	String cube;

	/**
	 * 
	 * @param cubeOrTable
	 *            the name of the queried structure.
	 * @param query
	 * @return
	 */
	public static AdhocQueryId from(String cubeOrTable, ICubeQuery query) {
		String queryHash = Integer.toHexString(query.toString().hashCode());
		AdhocQueryIdBuilder builder = AdhocQueryId.builder().queryHash(queryHash).cube(cubeOrTable);

		if (query instanceof IHasParentQueryId hasParentQueryId) {
			builder.parentQueryId(hasParentQueryId.getParentQueryId().getQueryId());
		}

		return builder.build();
	}
}
