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
