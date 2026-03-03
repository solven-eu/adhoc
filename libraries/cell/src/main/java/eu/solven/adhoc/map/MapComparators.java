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
package eu.solven.adhoc.map;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import lombok.experimental.UtilityClass;

/**
 * Various {@link Comparator} for {@link Map}.
 * 
 * @author Benoit Lacelle
 *
 */
@UtilityClass
public class MapComparators {
	public static <M extends NavigableMap> Comparator<M> navigableMapComparator() {
		return (Comparator<M>) NavigableMapComparator.INSTANCE;
	}

	/**
	 * BEWARE This {@link Comparator} is quite slow, as it may make two {@link HashMap} on each comparison.
	 * 
	 * @return
	 */
	public static <M extends Map> Comparator<M> mapComparator() {
		// Various casts are here to help javac doing its job
		return Comparator.comparing(map -> asNavigableMap((Map) map), (Comparator) NavigableMapComparator.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	private static <K, V> NavigableMap<K, V> asNavigableMap(Map<K, V> map) {
		if (map instanceof NavigableMap<?, ?> navigableMap) {
			return (NavigableMap<K, V>) navigableMap;
		} else {
			return new TreeMap<>(map);
		}
	}
}
