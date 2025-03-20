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
package eu.solven.adhoc.data.row;

import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.data.cell.IValueReceiver;
import eu.solven.adhoc.table.IAdhocTableWrapper;
import eu.solven.adhoc.table.transcoder.IAdhocTableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManager;

/**
 * Used to separate aggregates from groupBy from {@link IAdhocTableWrapper}
 * 
 * @author Benoit Lacelle
 */
public interface ITabularRecord {
	Set<String> aggregateKeySet();

	void onAggregate(String aggregateName, IValueReceiver valueConsumer);

	@Deprecated(since = "Prefer `void onAggregate(String aggregateName, IValueConsumer valueConsumer)`")
	Object getAggregate(String aggregateName);

	Set<String> groupByKeySet();

	Object getGroupBy(String columnName);

	/**
	 * 
	 * @return a merged {@link Map}. Ambiguities will pops if a name if both an aggregate and a groupBy.
	 */
	Map<String, ?> asMap();

	boolean isEmpty();

	Map<String, ?> getGroupBys();

	ITabularRecord transcode(IAdhocTableReverseTranscoder transcodingContext);

	ITabularRecord transcode(ICustomTypeManager customTypeManager);
}
