package eu.solven.adhoc.engine.step;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import eu.solven.adhoc.measure.model.Aggregator;

public class TestCubeQueryStep {
	@Test
	public void testGuavaCacheAsCache() {
		Cache<Object, Object> cache = CacheBuilder.newBuilder().build();
		CubeQueryStep step = CubeQueryStep.builder().cache(cache.asMap()).measure(Aggregator.countAsterisk()).build();

		step.getCache().put("k", "v");
		LocalDate today = LocalDate.now();
		step.getCache().put(today, 12.34D);
		Assertions.assertThat(step.getCache()).containsEntry("k", "v").containsEntry(today, 12.34D);
	}
}
