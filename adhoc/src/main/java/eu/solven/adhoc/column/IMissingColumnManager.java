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
package eu.solven.adhoc.column;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.table.composite.CompositeCubesTableWrapper;

/**
 * On edge-cases (e.g. a failed JOIN), we would encounter NULL. This interface centralizes the behaviors on such cases.
 * 
 * @author Benoit Lacelle
 */
public interface IMissingColumnManager {

	/**
	 * Typically called on {@link CompositeCubesTableWrapper}, when a {@link ICubeWrapper} is missing a column requested
	 * by the query.
	 * 
	 * @param cube
	 * @param column
	 * @return
	 */
	Object onMissingColumn(ICubeWrapper cube, String column);

	/**
	 * Typically called by {@link ICubeQueryEngine}, when received a row from table missing some column (e.g. failed
	 * JOIN).
	 * 
	 * @param column
	 * @return
	 */
	// BEWARE This should probably be contextual to the IAdhocTableWrapper
	Object onMissingColumn(String column);

}
