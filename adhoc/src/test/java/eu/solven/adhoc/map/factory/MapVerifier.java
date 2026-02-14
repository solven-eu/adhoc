package eu.solven.adhoc.map.factory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;

import lombok.Builder;
import lombok.NonNull;

/**
 * Helps verifying a {@link Map} instance follows standard {@link Map} contract.
 * 
 * @author Benoit Lacelle
 */
public class MapVerifier {
	@Builder
	public static class MapVerifierInstance {
		@NonNull
		Map<?, ?> toVerify;

		// TODO handle recursive custom Map as key or value
		public void verify() {
			Map<?, ?> rawMap = new LinkedHashMap<>(toVerify);
			Assertions.assertThat((Map) toVerify)
					.hasToString(rawMap.toString())
					.isEqualTo(rawMap)
					.hasSameHashCodeAs(rawMap);

			Set<?> rawKeySet = rawMap.keySet();
			Assertions.assertThat(toVerify.keySet())
					.hasToString(rawKeySet.toString())
					.isEqualTo(rawKeySet)
					.hasSameHashCodeAs(rawKeySet);

			Set<?> rawEntrySet = rawMap.entrySet();
			Assertions.assertThat(toVerify.entrySet())
					.hasToString(rawEntrySet.toString())
					.isEqualTo(rawEntrySet)
					.hasSameHashCodeAs(rawEntrySet);
		}
	}

	public static MapVerifierInstance forInstance(Map<?, ?> map) {
		return MapVerifierInstance.builder().toVerify(map).build();
	}
}
