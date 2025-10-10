package eu.solven.adhoc.data.row.slice;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.MaskedAdhocMap;
import eu.solven.adhoc.map.StandardSliceFactory;

public class TestMaskedSliceAsMap {
	@Test
	public void testEquals() {
		IAdhocMap decorated = StandardSliceFactory.fromMap(StandardSliceFactory.builder().build(), Map.of("a", "a1"));
		Map<String, ?> mask = Map.of("b", "b2");
		MaskedAdhocMap masked = MaskedAdhocMap.builder().decorated(decorated).mask(mask).build();

		// `.equals` any Map
		Assertions.assertThat((Map) masked).isEqualTo(Map.of("a", "a1", "b", "b2"));
		Assertions.assertThat(masked.hashCode()).isEqualTo(Map.of("a", "a1", "b", "b2").hashCode());

		// `.get` any key
		Assertions.assertThat(masked.get("a")).isEqualTo("a1");
		Assertions.assertThat(masked.get("b")).isEqualTo("b2");
		Assertions.assertThat(masked.get("c")).isEqualTo(null);
	}
}
