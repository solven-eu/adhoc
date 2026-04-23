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
package eu.solven.adhoc.util.cache;

import eu.solven.adhoc.util.immutable.IImmutable;

/**
 * Immutable pair of a reference-key and its cached value. Shared between {@link LastLookupCache},
 * {@link LastLookupCache1}, and {@link LastLookupCache1Volatile} as the last-entry holder.
 *
 * <p>
 * The fields are {@code final} and package-private so the enclosing cache classes can read them directly on the fast
 * path, without an accessor call. Final fields guarantee safe publication: a racing reader never sees a half-published
 * entry (fresh key paired with stale value). The {@link IImmutable} marker documents the intent; the Error Prone
 * {@code @Immutable} annotation is intentionally NOT used because {@code K} (often {@code Object[]}) is mutable in
 * practice — callers rely on reference equality and never mutate the array once it has been handed to the cache.
 *
 * <p>
 * {@code K} is {@code Object[]} for {@link LastLookupCache} and a plain {@code Object} (typically a single reference
 * key) for the length-1 variants.
 *
 * @param <K>
 *            the reference-key type (often {@code Object} or {@code Object[]})
 * @param <V>
 *            the cached value type
 * @author Benoit Lacelle
 */
public final class CacheEntry<K, V> implements IImmutable {

	final K key;
	final V value;

	public CacheEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}
}
