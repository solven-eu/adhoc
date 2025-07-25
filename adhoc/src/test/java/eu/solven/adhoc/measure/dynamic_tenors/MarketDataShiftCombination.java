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

import java.util.Optional;

import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import lombok.extern.slf4j.Slf4j;

/**
 * Simulate a service providing a MarketData shift (i.e. the variation (in nominal) of a MarketData between 2 given
 * instants).
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class MarketDataShiftCombination implements ICombination, IExamplePnLExplainConstant {
	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		IAdhocFilter filter = slice.asFilter();
		if (IAdhocFilter.MATCH_NONE.equals(filter)) {
			return IValueProvider.NULL;
		}

		Optional<String> optTenor =
				EqualsMatcher.extractOperand(FilterHelpers.getValueMatcher(filter, K_TENOR), String.class);
		if (optTenor.isEmpty()) {
			return IValueProvider.setValue("Lack tenor");
		}
		Optional<String> optMaturity =
				EqualsMatcher.extractOperand(FilterHelpers.getValueMatcher(filter, K_MATURITY), String.class);
		if (optMaturity.isEmpty()) {
			return IValueProvider.setValue("Lack maturity");
		}

		// [-99, 99]
		int deterministicRandomHash = (optTenor.get() + optMaturity.get()).hashCode() % 100;
		// [-9.9, 9.9]
		double deterministicRandomMarketDataShift = deterministicRandomHash / 10D;

		return IValueProvider.setValue(deterministicRandomMarketDataShift);
	}
}
