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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.sum.ProductAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * An example {@link ICombination} implementing a Foreign-Exchange business logic. This would fit only simpler
 * business-cases: you should probably implement your own, possibly using this as a referential-implementation.
 */
@Slf4j
@RequiredArgsConstructor
public class ForeignExchangeCombination implements ICombination {
	public static final String KEY = "FX";

	final IForeignExchangeStorage foreignExchangeStorage;

	protected Object getFX(IForeignExchangeStorage.FXKey fxKey) {
		return foreignExchangeStorage.getFX(fxKey);
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		Object beforeFx = underlyingValues.getFirst();

		if (!AdhocPrimitiveHelpers.isDoubleLike(beforeFx)) {
			// This is typically a String holding an error message
			return beforeFx;
		}

		// The ccy of the input value is determined by the slice providing the value
		String ccyFrom = slice.getSlice().getRawSliced("ccyFrom").toString();

		// The ccy of the output value is determined dynamically
		String ccyTo = getCcyTo(slice);

		IForeignExchangeStorage.FXKey fxKey =
				IForeignExchangeStorage.FXKey.builder().fromCcy(ccyFrom).toCcy(ccyTo).build();
		Object fxRate = getFX(fxKey);

		if (slice.getQueryStep().isDebug()) {
			log.info("[DEBUG] fxRate={} for fxKey={}", fxRate, fxKey);
		}

		if (!AdhocPrimitiveHelpers.isDoubleLike(fxRate)) {
			// This is typically a String holding an error message
			return fxRate;
		}

		return getProductAggregator().aggregate(beforeFx, fxRate);
	}

	protected ProductAggregation getProductAggregator() {
		return new ProductAggregation(Map.of());
	}

	protected String getCcyTo(ISliceWithStep slice) {
		// First, we consider a ccyTo configured as a customMarker: these are
		// configurable dynamically by the user
		Optional<?> optCustomMarker = slice.getQueryStep().optCustomMarker();
		if (optCustomMarker.isPresent()) {
			Object customMarker = optCustomMarker.get();

			if (customMarker instanceof String && customMarker.toString().matches("[A-Z]{3}")) {
				return customMarker.toString();
			} else if (customMarker instanceof Map<?, ?> customMarkerAsMap && customMarkerAsMap.containsKey("ccyTo")) {
				return MapPathGet.getRequiredString(customMarkerAsMap, "ccyTo");
			}
		}

		// Else, we try inferring a nice ccyTo from the measure name
		String measureName = slice.getQueryStep().getMeasure().getName();
		List<String> candidates = Pattern.compile("\\b[A-Z]{3}\\b")
				.matcher(measureName)
				.results()
				.map(MatchResult::group)
				.distinct()
				// `CCY` is the default measure suffix: it should not match as a given ccy
				.filter(ccy -> !"CCY".equals(ccy))
				.toList();
		if (candidates.size() == 1) {
			return candidates.getFirst();
		} else if (candidates.size() >= 2) {
			log.debug("There is multiple ccy candidates in {}: {}", measureName, candidates);
		}

		// Let's say we want EUR as default ccy
		return getDefaultCcyTo();
	}

	protected String getDefaultCcyTo() {
		return "EUR";
	}
}
