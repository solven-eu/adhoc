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
package eu.solven.adhoc.query.foreignexchange;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This represents an extraneous storage of ForeignExchange rates.
 */
public class ForeignExchangeStorage implements IForeignExchangeStorage {

	final Map<FXKey, Double> fxKeyToRate = new ConcurrentHashMap<>();

	@Override
	public Object getFX(FXKey fxKey) {
		if (fxKey.getFromCcy().equals(fxKey.getToCcy())) {
			// FX with same currency: the rate is always 1D as no rate to apply
			return 1D;
		} else {
			Double rate = getRateMayInverse(fxKey);

			// FX(EUR->JPY) is equal to FX(EUR->USD) * FX(USD->JPY)
			// This is relevant as most CCY has an FX with USD
			if (rate == null) {
				String pivotCcy = "USD";

				Double rateFromToUsd = getRateMayInverse(fxKey.toBuilder().toCcy(pivotCcy).build());
				Double rateUsdToTo = getRateMayInverse(fxKey.toBuilder().fromCcy(pivotCcy).build());

				if (rateFromToUsd != null && rateUsdToTo != null) {
					rate = rateFromToUsd * rateUsdToTo;
				}
			}

			if (rate == null) {
				return "Missing_FX_Rate-%s-%s-%s".formatted(fxKey.getAsOf(), fxKey.getFromCcy(), fxKey.getToCcy());
			} else {
				return rate;
			}
		}
	}

	private Double getRateMayInverse(FXKey fxKey) {
		Double rate = fxKeyToRate.get(fxKey);

		if (rate == null) {
			Double inversedRate = fxKeyToRate.get(fxKey.inverse());

			if (inversedRate != null) {
				// We inverse the rate as we requested the inverse ccyPair
				rate = 1D / inversedRate;
			}
		}
		return rate;
	}

	public void addFx(FXKey fxKey, double rate) {
		fxKeyToRate.put(fxKey, rate);
	}
}
