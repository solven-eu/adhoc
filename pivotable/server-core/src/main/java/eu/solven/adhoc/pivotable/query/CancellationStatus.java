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
package eu.solven.adhoc.pivotable.query;

import lombok.RequiredArgsConstructor;

/**
 * The different state of a query execution.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public enum CancellationStatus {
	// the queryId is unknown
	UNKNOWN(AsynchronousStatus.UNKNOWN),
	// the query is running
	CANCELLED(AsynchronousStatus.RUNNING),
	// the query is completed and its result is available
	SERVED(AsynchronousStatus.SERVED),
	// the query ended with a failure
	FAILED(AsynchronousStatus.FAILED),
	// the view is not available anymore, or it has been cancelled
	DISCARDED(AsynchronousStatus.DISCARDED);

	final AsynchronousStatus asyncStatus;
}