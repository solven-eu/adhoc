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
package eu.solven.adhoc.table;

import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.cache.CachingTableWrapper;

/**
 * Helps managing how a customMarker would impact the cache.
 * 
 * Typical usage is in {@link ITableWrapper} and {@link CachingTableWrapper}: a typical cache key is
 * {@link TableQueryV2}, but the received cutomerMarker is generally without impact.
 * 
 * @author Benoit Lacelle
 */
public interface ICustomMarkerCacheStrategy {
	/**
	 * 
	 * @param customMarker
	 *            some customMarker, typically provided by the user
	 * @return a potentially simpler customMarker, used in the key of related cache.
	 */
	Object restrictToCacheImpact(Object customMarker);
}
