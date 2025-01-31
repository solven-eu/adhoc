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
package eu.solven.adhoc.aggregations.many_to_many;

import java.util.Set;

import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Simplest many2many relies on a single input columns, and equals matching.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IManyToMany1DDefinition {
	/**
	 * @param element
	 * @return the groups including given element
	 */
	Set<Object> getGroups(Object element);

	/**
	 *
	 * @param groupMatcher
	 * @return the elements which group is matched
	 */
	Set<?> getElementsMatchingGroups(IValueMatcher groupMatcher);

	Set<?> getMatchingGroups(IValueMatcher groupMatcher);
}
