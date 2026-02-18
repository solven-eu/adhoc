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

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Standard implementation of {@link IHasIndexOf}.
 * 
 * It enables faster evaluation of `.indexOf` given a `Collection`.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CollectionWithCustomIndexOf<T> implements IHasIndexOf<T> {
	@Getter
	// @Singular
	@NonNull
	final Collection<T> keys;
	@NonNull
	final IHasIndexOf<T> hash;

	@Override
	public int indexOf(T key) {
		return hash.indexOf(key);
	}

	/**
	 * Lombok @Builder
	 */
	public static class CollectionWithCustomIndexOfBuilder<T> {
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		IHasIndexOfFactory<T> factory;
		Collection<T> keys;
		IHasIndexOf<T> hash;

		public CollectionWithCustomIndexOfBuilder<T> factory(IHasIndexOfFactory<T> factory) {
			this.factory = factory;

			return this;
		}

		/**
		 * Relies on a {@link HashMap} for the `key->index` mapping
		 * 
		 * @return
		 */
		public CollectionWithCustomIndexOfBuilder<T> hashMap() {
			return this.factory(HashMapIndexOf::make);
		}

		/**
		 * Relies on a {@link SimplePerfectHash} for the `key->index` mapping
		 * 
		 * @return
		 */
		public CollectionWithCustomIndexOfBuilder<T> perfectHash() {
			return this.factory(SimplePerfectHash::make);
		}

		/**
		 * Relies on a {@link SimplePerfectHash} for the `key->index` mapping
		 * 
		 * @return
		 */
		public CollectionWithCustomIndexOfBuilder<T> unsafePerfectHash() {
			return this.factory(keys -> SimplePerfectHash.make(keys)::unsafeIndexOf);
		}

		public CollectionWithCustomIndexOf<T> build() {
			if (this.hash == null && this.factory == null) {
				return this.hashMap().build();
			} else if (this.hash == null) {
				this.hash = this.factory.makeHasIndexOf(keys);
			}

			return new CollectionWithCustomIndexOf<T>(this.keys, this.hash);
		}

	}
}
