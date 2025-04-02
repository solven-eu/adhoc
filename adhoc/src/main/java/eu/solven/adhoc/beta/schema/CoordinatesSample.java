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
package eu.solven.adhoc.beta.schema;

import com.google.common.collect.ImmutableSet;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;

/**
 * Enables providing metadata about a column, especially a sample of available coordinates and an estimated cardinality.
 * 
 * The provided coordinates is a sample, given some limit. The estimatedVardinality is not impacted by this limit.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class CoordinatesSample {
	public static final long NO_ESTIMATION = -1;

	@Singular
	ImmutableSet<Object> coordinates;

	@Default
	long estimatedCardinality = NO_ESTIMATION;

	public long getEstimatedCardinality() {
		return estimatedCardinality;
	}

	public static CoordinatesSample empty() {
		return CoordinatesSample.builder().build();
	}
}
