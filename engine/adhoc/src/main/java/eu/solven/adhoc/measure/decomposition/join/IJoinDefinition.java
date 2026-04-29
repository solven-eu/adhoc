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
package eu.solven.adhoc.measure.decomposition.join;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.column.IHasColumns;

/**
 * Backing data for a {@link JoinDecomposition}: maps a composite key (input columns → values) to a row of output
 * columns. Each lookup returns a {@code Map<String, Object>} holding one value per output column.
 *
 * @author Benoit Lacelle
 */
public interface IJoinDefinition extends IHasColumns {

	/**
	 * @param joinKey
	 *            a map from input-column name to coordinate value.
	 * @return the output row for this key (one entry per output column), or empty if there is no match.
	 */
	Optional<Map<String, Object>> lookup(Map<String, Object> joinKey);

	/**
	 * @param outputColumn
	 *            the name of one output column.
	 * @return all distinct values for that output column. Used for {@code getCoordinates}.
	 */
	Set<Object> allOutputValues(String outputColumn);
}
