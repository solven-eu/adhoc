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
package eu.solven.adhoc.map.factory;

import java.util.Map;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ASliceFactory.IHasEntries;
import eu.solven.adhoc.measure.transformator.iterator.IDagBottomUpStrategy;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;

/**
 * Enable building {@link Map} and {@link IAdhocSlice} in Adhoc context.
 * 
 * In Adhoc, we generate tons of {@link Map}-like for a given {@link IAdhocGroupBy}. Which means a tons of
 * {@link Map}-like for a predefined keySet. Given {@link Map} may be sorted, to enable faster merging (see
 * {@link IDagBottomUpStrategy}).
 * 
 * @author Benoit Lacelle
 */
public interface ISliceFactory {

	/**
	 * 
	 * @param keys
	 * @return a {@link IMapBuilderPreKeys} for given set of keys.
	 */
	IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys);

	/**
	 * BEWARE This should be used in last resort, when the keySet is not available.
	 * 
	 * @return an {@link IMapBuilderThroughKeys}, which can handle a stream of entries where the keySet is not known in
	 *         advance.
	 */
	@Deprecated
	IMapBuilderThroughKeys newMapBuilder();

	IAdhocMap buildMap(IHasEntries hasEntries);
}
