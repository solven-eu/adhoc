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
package eu.solven.adhoc.map.perfect_hashing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

/**
 * {@link IHasIndexOf} based on a {@link HashMap}.
 * 
 * @author Benoit Lacelle
 * @param <T>
 */
@RequiredArgsConstructor
public class HashMapIndexOf<T> implements IHasIndexOf<T> {
	final Map<T, Integer> asMap;

	public static <T> IHasIndexOf<T> make(Collection<T> keys) {
		Map<T, Integer> asMap = HashMap.newHashMap(keys.size());
		keys.forEach(key -> asMap.put(key, asMap.size()));

		return new HashMapIndexOf<>(asMap);
	}

	@Override
	public int indexOf(T key) {
		return asMap.getOrDefault(key, -1);
	}

}