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

import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

/**
 * Immutable
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class MarketRiskSensitivity {
	// Typically `tenor->1Y&maturity->2Y`
	@Default
	Object2DoubleMap<Map<String, ?>> coordinatesToDelta = new Object2DoubleOpenHashMap<>();

	public MarketRiskSensitivity mergeWith(MarketRiskSensitivity other) {
		Object2DoubleMap<Map<String, ?>> merged = new Object2DoubleOpenHashMap<>(coordinatesToDelta);

		other.getCoordinatesToDelta().object2DoubleEntrySet().forEach(e -> {
			merged.mergeDouble(e.getKey(), e.getDoubleValue(), (l, r) -> l + r);
		});

		return MarketRiskSensitivity.builder().coordinatesToDelta(merged).build();
	}

	public static MarketRiskSensitivity empty() {
		return MarketRiskSensitivity.builder().coordinatesToDelta(Object2DoubleMaps.emptyMap()).build();
	}

	public MarketRiskSensitivity addDelta(Map<String, ?> map, double delta) {
		Object2DoubleMap<Map<String, ?>> merged = new Object2DoubleOpenHashMap<>(coordinatesToDelta);

		merged.mergeDouble(map, delta, (l, r) -> l + r);

		return MarketRiskSensitivity.builder().coordinatesToDelta(merged).build();
	}
}
