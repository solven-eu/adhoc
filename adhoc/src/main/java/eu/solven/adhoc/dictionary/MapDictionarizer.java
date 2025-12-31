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
package eu.solven.adhoc.dictionary;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Simple {@link IDictionarizer} based on an {@link ConcurrentHashMap}.
 * 
 * @author Benoit Lacelle
 */
// https://stackoverflow.com/questions/29826787/is-this-dictionary-function-thread-safe-concurrenthashmapatomicinteger
public class MapDictionarizer implements IDictionarizer {
	final ConcurrentMap<Object, Integer> objectToInt = new ConcurrentHashMap<>();
	final List<Object> intToObject = new CopyOnWriteArrayList<>();

	@Override
	public Object fromInt(int indexedValue) {
		return intToObject.get(indexedValue);
	}

	@Override
	public int toInt(Object object) {
		// ConcurrentHashMap guarantees the `ifAbsent` function to be called zero or once per `computeIfAbsent`
		// invocation
		return objectToInt.computeIfAbsent(object, o -> {
			intToObject.add(o);

			return intToObject.size() - 1;
		});
	}

}
