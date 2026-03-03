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
package eu.solven.adhoc.util.immutable;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.encoding.page.row.ILikeList;
import lombok.experimental.UtilityClass;

/**
 * Helps working with immutable data-structures
 * 
 * @author Benoit Lacelle
 * 
 */
@UtilityClass
public class ImmutableHelpers {

	/**
	 * 
	 * @param map
	 * @return an immutable copy of the input, which may or may not be an {@link IAdhocMap}
	 */
	public static Map<String, ?> copyOf(Map<String, ?> map) {
		if (map instanceof IImmutable) {
			// For performance, we expect to be generally in this branch
			// IAdhocMap are immutable but they do not extend ImmutableMap
			return map;
		} else {
			return ImmutableMap.copyOf(map);
		}
	}

	@SuppressWarnings("unchecked") // all supported methods are covariant
	public static <T> List<T> copyOf(Iterable<? extends T> iterable) {
		if (iterable instanceof ILikeList likeList) {
			// For performance, we expect to be generally in this branch
			return ImmutableList.copyOf(likeList.asList());
		} else if (iterable instanceof IImmutable && iterable instanceof List list) {
			return list;
		} else {
			return ImmutableList.copyOf(iterable);
		}
	}
}
