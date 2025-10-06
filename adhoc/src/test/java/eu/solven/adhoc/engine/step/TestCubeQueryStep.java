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
package eu.solven.adhoc.engine.step;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

	@Test
	public void testCrossStepsCache() {
		CubeQueryStep step = CubeQueryStep.builder().measure("m").build();

		// Issue if the cache is not initializer
		Assertions.assertThatThrownBy(() -> step.getTransverseCache()).isInstanceOf(IllegalStateException.class);

		// Initialize the cache
		ConcurrentMap<Object, Object> transverseCache = new ConcurrentHashMap<>();
		step.setCrossStepsCache(transverseCache);

		// Check the cache is properly behaving
		Assertions.assertThat(step.getTransverseCache()).isSameAs(transverseCache);

		// transverseCache is still valid after an `invalidateAll`
		step.invalidateAll();
		Assertions.assertThat(step.getTransverseCache()).isSameAs(transverseCache);
	}
}
