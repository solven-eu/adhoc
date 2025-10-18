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

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.solven.adhoc.data.tabular.IReadableTabularView;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Holds the details for the queryResult API
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
@JsonInclude(Include.NON_NULL)
public class QueryResultHolder {
	@NonNull
	AsynchronousStatus state;

	IReadableTabularView view;

	Duration retryIn;

	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	public Long getRetryInMs() {
		// Make it easier in language like Javascript to get the retry duration
		if (retryIn == null) {
			return null;
		}
		return retryIn.toMillis();
	}

	public static QueryResultHolder served(AsynchronousStatus state, IReadableTabularView view) {
		return QueryResultHolder.builder().state(state).view(view).build();
	}

	public static QueryResultHolder retry(AsynchronousStatus state, Duration retryIn) {
		return QueryResultHolder.builder().state(state).retryIn(retryIn).build();
	}

	// May have been cancelled, or done but result is discarded
	public static QueryResultHolder discarded(AsynchronousStatus state) {
		return QueryResultHolder.builder().state(state).build();
	}
}
