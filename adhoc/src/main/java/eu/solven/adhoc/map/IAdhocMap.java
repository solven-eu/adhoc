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

import java.util.HashMap;
import java.util.Map;

import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.slice.IAdhocSlice;

/**
 * A map dedicated to Adhoc. It is typically used to expressed a {@link IAdhocSlice} given a{@link IAdhocGroupBy}.
 * 
 * It requires {@link String} keys and {@link Object} values, as columns are always String.
 * 
 * It is immutable as it is used as key in {@link Map}. it may typically cache the hashCode.
 * 
 * It is {@link Comparable} to enables {@link HashMap} optimizations on hashCode collisions
 * (https://openjdk.org/jeps/180).
 * 
 * @author Benoit Lacelle
 */
public interface IAdhocMap extends Map<String, Object>
// , Comparable<AdhocMap>
{

}
