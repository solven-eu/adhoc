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
package eu.solven.adhoc.data.cell;

import java.math.BigDecimal;
import java.math.BigInteger;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps working with {@link IValueProvider} for some advanced cases. Some helpers are available in
 * {@link IValueProvider}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
public class ValueProviderHelpers {

	@SuppressWarnings("PMD.UnnecessaryBoxing")
	public static IValueProvider asLongIfExact(Number number) {
		if (number instanceof Integer intValue) {
			return IValueProvider.setValue(intValue.intValue());
		} else if (number instanceof Long longValue) {
			return IValueProvider.setValue(longValue.longValue());
		} else if (number instanceof BigInteger bigInteger) {
			try {
				long asLong = bigInteger.longValueExact();
				if (log.isTraceEnabled()) {
					log.trace("This is a long: {} -> {}", bigInteger, asLong);
				}
				return IValueProvider.setValue(asLong);
			} catch (ArithmeticException e) {
				log.trace("This is not a long: {}", bigInteger, e);
			}
		} else if (number instanceof BigDecimal bigDecimal) {
			try {
				long asLong = bigDecimal.longValueExact();
				if (log.isTraceEnabled()) {
					log.trace("This is a long: {} -> {}", bigDecimal, asLong);
				}
				return IValueProvider.setValue(asLong);
			} catch (ArithmeticException e) {
				log.trace("This is not a long: {}", bigDecimal, e);
			}
		}
		return IValueProvider.setValue(number.doubleValue());
	}
}
