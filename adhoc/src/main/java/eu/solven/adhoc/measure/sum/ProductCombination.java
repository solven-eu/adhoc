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
package eu.solven.adhoc.measure.sum;

import java.util.List;
import java.util.Map;

import com.google.common.base.Functions;

import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * A {@link ICombination} which multiplies the underlying measures. If any measure is null, the product is null.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/product-function-dax
public class ProductCombination implements ICombination {

	public static final String KEY = "PRODUCT";

	// If true, a null underlying leads to a null output
	// If false, null underlyings are ignored
	final boolean nullOperandIsNull;

	public ProductCombination() {
		nullOperandIsNull = true;
	}

	@Override
	public Object combine(List<?> underlyingValues) {
		if (nullOperandIsNull && underlyingValues.contains(null)) {
			return null;
		}

		return underlyingValues.stream()
				.<Object>map(Functions.identity())
				.reduce(new ProductAggregator()::aggregate)
				.orElse(null);
	}

	public ProductCombination(Map<String, ?> options) {
		nullOperandIsNull = MapPathGet.<Boolean>getOptionalAs(options, "nullOperandIsNull").orElse(true);
	}

}
