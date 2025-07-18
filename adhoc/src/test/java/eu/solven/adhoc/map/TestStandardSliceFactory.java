package eu.solven.adhoc.map;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestStandardSliceFactory {
	StandardSliceFactory factory = StandardSliceFactory.builder().build();

	@Test
	public void isNorOrdered() {
		Assertions.assertThat(factory.isNotOrdered(Set.of("a"))).isTrue();

		{
			HashSet<String> hashSet = new HashSet<>();
			Assertions.assertThat(factory.isNotOrdered(hashSet)).isFalse();
		}

		// HashMap keySet is considered ordered, as we expect to iterate in `.values` which has same ordering as
		// `.keySet`
		{
			HashMap<String, Object> hashMap = new HashMap<>();
			Assertions.assertThat(factory.isNotOrdered(hashMap.keySet())).isFalse();
		}

		// Empty so ordered
		Assertions.assertThat(factory.isNotOrdered(Set.of())).isFalse();
		// These implementations are ordered
		Assertions.assertThat(factory.isNotOrdered(List.of())).isFalse();
		Assertions.assertThat(factory.isNotOrdered(ImmutableList.of())).isFalse();
		Assertions.assertThat(factory.isNotOrdered(ImmutableSet.of())).isFalse();
	}
}
