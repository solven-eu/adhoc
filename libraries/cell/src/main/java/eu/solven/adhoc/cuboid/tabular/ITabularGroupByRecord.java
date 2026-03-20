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
package eu.solven.adhoc.cuboid.tabular;

import java.util.NavigableSet;
import java.util.Set;

import eu.solven.adhoc.cuboid.slice.IHasSlice;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasGroupBy;

/**
 * Used to hold the slice (given the groupBy) from {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
public interface ITabularGroupByRecord extends IHasSlice, IHasGroupBy, ITabularGroupBySlice {
	/**
	 * Column names may not be sufficient, especially given ICalculatedColumn.
	 * 
	 * @return the {@link IGroupBy} which has produced this column.
	 */
	@Override
	IGroupBy getGroupBy();

	/**
	 * The columns for which a coordinate is expressed
	 */
	@Override
	Set<String> columnsKeySet();

	ITabularGroupByRecord retainAll(NavigableSet<String> columns);

}
