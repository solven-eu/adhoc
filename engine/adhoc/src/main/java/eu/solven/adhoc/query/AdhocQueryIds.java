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
package eu.solven.adhoc.query;

import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.cube.IHasParentQueryId;
import lombok.experimental.UtilityClass;

/**
 * Cube-aware factory helpers for {@link AdhocQueryId}. Lives in the cube layer because it depends on {@link ICubeQuery}
 * / {@link IHasParentQueryId}; keeping it separate from the data class itself lets the table layer reach the data class
 * without pulling cube types.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocQueryIds {

	/**
	 * Build an {@link AdhocQueryId} from a cube/table name and an {@link ICubeQuery}, deriving a queryHash from the
	 * query's {@code toString()} and a parentQueryId when the query implements {@link IHasParentQueryId}.
	 */
	public static AdhocQueryId from(String cubeOrTable, ICubeQuery query) {
		String queryHash = Integer.toHexString(query.toString().hashCode());
		AdhocQueryId.AdhocQueryIdBuilder builder = AdhocQueryId.builder().queryHash(queryHash).cube(cubeOrTable);

		if (query instanceof IHasParentQueryId hasParentQueryId) {
			builder.parentQueryId(hasParentQueryId.getParentQueryId().getQueryId());
		}

		return builder.build();
	}
}
