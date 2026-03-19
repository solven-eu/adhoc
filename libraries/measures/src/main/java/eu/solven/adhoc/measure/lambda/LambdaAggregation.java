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
package eu.solven.adhoc.measure.lambda;

import java.util.Map;
import java.util.function.BiFunction;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.util.map.AdhocMapPathGet;

/**
 * Enable an {@link IAggregation} to be defined through a lambda. Beware this is typically not serializable.
 * 
 * @author Benoit Lacelle
 */
public class LambdaAggregation implements IAggregation {
	public static final String K_LAMBDA = "lambda";

	final ILambdaAggregation lambda;

	/**
	 * Minimal contract of a lambda to define an {@link IAggregation}
	 */
	@FunctionalInterface
	public interface ILambdaAggregation extends BiFunction<Object, Object, Object> {

	}

	public LambdaAggregation(Map<String, ?> options) {
		lambda = AdhocMapPathGet.getRequiredAs(options, K_LAMBDA);
	}

	@Override
	public Object aggregate(Object left, Object right) {
		return lambda.apply(left, right);
	}
}
