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
package eu.solven.adhoc.measure.dynamic_tenors;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.DoubleAdder;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Sum the sensitivity of input {@link MarketRiskSensitivity}, restricting to filtered tenors and maturities.
 * 
 * @author Benoit Lacelle
 */
public class ReduceSensitivitiesCombination implements ICombination, IExamplePnLExplainConstant {
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		MarketRiskSensitivity sensitivities = (MarketRiskSensitivity) underlyingValues.getFirst();

		IValueMatcher tenorMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_TENOR);
		IValueMatcher maturityMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_MATURITY);

		DoubleAdder accumulated = new DoubleAdder();
		AtomicBoolean oneContributed = new AtomicBoolean();

		sensitivities.getCoordinatesToDelta().object2DoubleEntrySet().forEach(e -> {
			Object tenor = e.getKey().get(K_TENOR);
			if (!tenorMatcher.match(tenor)) {
				return;
			}
			Object maturity = e.getKey().get(K_MATURITY);
			if (!maturityMatcher.match(maturity)) {
				return;
			}

			accumulated.add(e.getDoubleValue());
			oneContributed.set(true);
		});

		if (oneContributed.get()) {
			return accumulated.doubleValue();
		} else {
			return null;
		}
	}

}
