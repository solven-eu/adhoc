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
import java.util.Map;
import java.util.NavigableMap;

import org.junit.jupiter.api.Test;

public class TestMapComparators {
	// Test to check the API over advanced generics
	@SuppressWarnings("unused")
	@Test
	public void testGenerics() {
		Comparator<Map<?, ?>> comparatorWildcardWildcard = MapComparators.mapComparator();
		Comparator<Map<String, ?>> comparatorStringWildcard = MapComparators.mapComparator();
		Comparator<Map<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<Map<String, ?>>mapComparator();
	}

	// Test to check the API over advanced generics
	@SuppressWarnings("unused")
	@Test
	public void testGenerics_mapIntonavigable() {
		Comparator<NavigableMap<?, ?>> comparatorWildcardWildcard = MapComparators.mapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcard = MapComparators.mapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<NavigableMap<String, ?>>mapComparator();
	}

	// Test to check the API over advanced generics
	@SuppressWarnings("unused")
	@Test
	public void testGenerics_navigableIntoNavigable() {
		Comparator<NavigableMap<?, ?>> comparatorWildcardWildcard = MapComparators.navigableMapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcard = MapComparators.navigableMapComparator();
		Comparator<NavigableMap<String, ?>> comparatorStringWildcardWithExplicit =
				MapComparators.<NavigableMap<String, ?>>navigableMapComparator();
	}
}
