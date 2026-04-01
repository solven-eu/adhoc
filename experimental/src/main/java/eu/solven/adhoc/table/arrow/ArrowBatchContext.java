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
package eu.solven.adhoc.table.arrow;

import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.engine.context.QueryPod;

/**
 * Immutable context shared by {@link ArrowBatchSpliterator} and every {@link ArrowFixedBatchSpliterator} derived from
 * it. Grouping these fields avoids re-passing them individually at every {@link ArrowFixedBatchSpliterator#trySplit()}
 * call.
 *
 * @param factory
 *            the record factory used to convert Arrow vectors to {@link eu.solven.adhoc.dataframe.row.ITabularRecord}
 * @param queryPod
 *            the query pod carrying cancellation state
 * @param minSplitRows
 *            minimum rows in a loaded batch before {@link ArrowFixedBatchSpliterator#trySplit()} will split it
 * @author Benoit Lacelle
 */
record ArrowBatchContext(ITabularRecordFactory factory,QueryPod queryPod,int minSplitRows){}
