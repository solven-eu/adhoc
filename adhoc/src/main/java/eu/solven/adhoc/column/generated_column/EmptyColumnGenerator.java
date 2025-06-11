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
package eu.solven.adhoc.column.generated_column;

import java.util.Map;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * An empty {@link IColumnGenerator}.
 * 
 * @author Benoit Lacelle
 */
public class EmptyColumnGenerator implements IColumnGenerator {

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return CoordinatesSample.empty();
	}

	/**
	 * Typically useful as default.
	 * 
	 * @return an empty {@link IColumnGenerator}.
	 */
	public static IColumnGenerator empty() {
		return new EmptyColumnGenerator();
	}

}
