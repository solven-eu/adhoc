/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.aggregation.collection;

import java.util.Map;

import eu.solven.adhoc.measure.aggregation.IAggregation;

/**
 * A generic `union`, including `Set` and `Map` unions.
 * 
 * @author Benoit Lacelle
 */
public class UnionAggregation implements IAggregation {

	public static final String KEY = "UNION";

	private final MapAggregation<Object, Object> mapAggregator = new MapAggregation<>();
	private final UnionSetAggregation setAggregator = new UnionSetAggregation();

	@Override
	public Object aggregate(Object l, Object r) {
		if (l == null) {
			return wrapOne(r);
		} else if (r == null) {
			return wrapOne(l);
		} else if (mapAggregator.acceptAggregate(l) && mapAggregator.acceptAggregate(r)) {
			return mapAggregator.aggregate(l, r);
		} else {
			return setAggregator.aggregate(l, r);
		}
	}

	protected Object wrapOne(Object input) {
		if (input == null) {
			return null;
		} else if (input instanceof Map<?, ?> map) {
			return map;
		} else {
			return setAggregator.aggregate(null, input);
		}
	}

}
