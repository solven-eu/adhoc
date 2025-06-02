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

import java.util.Map;

import eu.solven.adhoc.data.cell.MultitypeCell;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * A {@link ICombination} which multiplies the underlying measures. If any measure is null, the product is null.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/product-function-dax
public class ProductCombination extends AggregationCombination {

	public static final String KEY = "PRODUCT";

	public ProductCombination() {
		super(new ProductAggregation(Map.of()), true);
	}

	public ProductCombination(Map<String, ?> options) {
		super(new ProductAggregation(options),
				MapPathGet.<Boolean>getOptionalAs(options, K_CUSTOM_IF_ANY_NULL_OPERAND).orElse(true));
	}

	@Override
	protected MultitypeCell makeMultitypeCell() {
		return MultitypeCell.builder().aggregation(agg).asLong(1L).asDouble(1D).build();
	}

}
