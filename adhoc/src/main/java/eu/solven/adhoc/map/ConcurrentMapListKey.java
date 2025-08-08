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
package eu.solven.adhoc.map;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tentative to implement a Trie to manage better Map with a List key.
 * 
 * @param <K>
 * @param <V>
 * 
 * @author Benoit Lacelle
 */
@Deprecated
public interface ConcurrentMapListKey<K, V> extends ConcurrentMap<List<K>, V> {

	/**
	 * Tentative to implement a Trie to manage better Map with a List key.
	 * 
	 * @param <K>
	 * @param <V>
	 * 
	 * @author Benoit Lacelle
	 */
	class TrieNode<K, V> {
		Map<K, TrieNode<K, V>> children = new ConcurrentHashMap<>();
		V value;
	}

	/**
	 * Tentative to implement a Trie to manage better Map with a List key.
	 * 
	 * @param <K>
	 * @param <V>
	 * 
	 * @author Benoit Lacelle
	 */
	class ListKeyTrieMap<K, V> {
		private final TrieNode<K, V> root = new TrieNode<>();

		public void put(List<K> key, V value) {
			TrieNode<K, V> node = root;
			for (K k : key) {
				node = node.children.computeIfAbsent(k, x -> new TrieNode<>());
			}
			node.value = value;
		}

		public V get(List<K> key) {
			TrieNode<K, V> node = root;
			for (K k : key) {
				node = node.children.get(k);
				if (node == null) {
					return null;
				}
			}
			return node.value;
		}
	}
}
