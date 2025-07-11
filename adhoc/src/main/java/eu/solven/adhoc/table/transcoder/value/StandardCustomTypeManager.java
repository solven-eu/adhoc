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
package eu.solven.adhoc.table.transcoder.value;

import java.util.Map;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;

/**
 * Default {@link ICustomTypeManager}.
 * 
 * @author Benoit Lacelle
 */
public class StandardCustomTypeManager implements ICustomTypeManager {

	@Override
	public boolean mayTranscode(String column) {
		return false;
	}

	@Override
	public Object toTable(String column, Object coordinate) {
		return coordinate;
	}

	@Override
	public Object fromTable(String column, Object coordinate) {
		return coordinate;
	}

	@Override
	public IValueMatcher toTable(String column, IValueMatcher valueMatcher) {
		throw new NotYetImplementedException(
				"valueMatcher typeTranscoding: %s".formatted(PepperLogHelper.getObjectAndClass(valueMatcher)));
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of();
	}
}
