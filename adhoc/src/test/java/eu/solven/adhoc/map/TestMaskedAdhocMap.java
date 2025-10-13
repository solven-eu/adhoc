package eu.solven.adhoc.map;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMaskedAdhocMap {
	StandardSliceFactory factory = StandardSliceFactory.builder().build();

	@Test
	public void testCompare_withSameMask() {
		IAdhocMap decorated1 = StandardSliceFactory.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		IAdhocMap decorated2 = StandardSliceFactory.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked2 = MaskedAdhocMap.builder().decorated(decorated2).mask(Map.of("k2", "v2")).build();

		Assertions.assertThat((Map) masked1).isEqualTo(masked2);
		Assertions.assertThat((Comparable) masked1).isEqualByComparingTo(masked2);
	}

	@Test
	public void testCompare_differentMask() {
		IAdhocMap decorated1 = StandardSliceFactory.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		IAdhocMap decorated2 = StandardSliceFactory.fromMap(factory, Map.of("k2", "v2"));
		MaskedAdhocMap masked2 = MaskedAdhocMap.builder().decorated(decorated2).mask(Map.of("k", "v")).build();

		Assertions.assertThat((Map) masked1).isEqualTo(masked2);
		Assertions.assertThat((Comparable) masked1).isEqualByComparingTo(masked2);
	}

	@Test
	public void testImmutable() {
		IAdhocMap decorated1 = StandardSliceFactory.fromMap(factory, Map.of("k", "v"));
		MaskedAdhocMap masked1 = MaskedAdhocMap.builder().decorated(decorated1).mask(Map.of("k2", "v2")).build();

		Assertions.assertThatThrownBy(() -> masked1.clear()).isInstanceOf(UnsupportedOperationException.class);
		// existing entry
		Assertions.assertThatThrownBy(() -> masked1.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
		// new key
		Assertions.assertThatThrownBy(() -> masked1.put("k3", "v3")).isInstanceOf(UnsupportedOperationException.class);
	}
}
