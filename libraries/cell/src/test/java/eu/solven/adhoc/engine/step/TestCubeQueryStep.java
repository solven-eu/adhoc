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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.ISliceFilter;

public class TestCubeQueryStep {
	@Test
	public void testGuavaCacheAsCache() {
		IMeasure m = Mockito.mock(IMeasure.class);

		Cache<Object, Object> cache = CacheBuilder.newBuilder().build();
		CubeQueryStep step = CubeQueryStep.builder().cache(cache.asMap()).measure(m).build();

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

		CubeQueryStep copy = step.toBuilder().build();
		Assertions.assertThat(copy.getTransverseCache()).isSameAs(transverseCache);
	}

	@Test
	public void testCustomMarker_notOptional() {
		CubeQueryStep stepExplicit = CubeQueryStep.builder().measure("m").customMarker("foo").build();
		Assertions.assertThat(stepExplicit.getCustomMarker()).isEqualTo("foo");

		CubeQueryStep stepOptional = CubeQueryStep.builder().measure("m").customMarker(Optional.of("foo")).build();
		Assertions.assertThat(stepOptional).isEqualTo(stepExplicit);

		CubeQueryStep stepToBuilderThenExplicit =
				CubeQueryStep.builder().measure("m").build().toBuilder().customMarker("foo").build();
		Assertions.assertThat(stepToBuilderThenExplicit).isEqualTo(stepExplicit);

		CubeQueryStep stepToBuilderThenOptional =
				CubeQueryStep.builder().measure("m").build().toBuilder().customMarker(Optional.of("foo")).build();
		Assertions.assertThat(stepToBuilderThenOptional).isEqualTo(stepExplicit);

		CubeQueryStep stepEditThenExplicit =
				CubeQueryStep.edit((IWhereGroupByQuery) CubeQueryStep.builder().measure("m").build())
						.customMarker("foo")
						.build();
		Assertions.assertThat(stepEditThenExplicit).isEqualTo(stepExplicit);

		CubeQueryStep stepEditThenOptional =
				CubeQueryStep.edit((IWhereGroupByQuery) CubeQueryStep.builder().measure("m").build())
						.customMarker(Optional.of("foo"))
						.build();
		Assertions.assertThat(stepEditThenOptional).isEqualTo(stepExplicit);
	}

	@Test
	public void testCustomMarker_emptyOptional() {
		CubeQueryStep stepOptional = CubeQueryStep.builder().measure("m").customMarker(Optional.empty()).build();
		Assertions.assertThat(stepOptional).isEqualTo(CubeQueryStep.builder().measure("m").build());
	}

	@Test
	public void testEdit() {
		CubeQueryStep step = CubeQueryStep.builder()
				// This test should customize all fields
				.measure(Mockito.mock(IMeasure.class))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
				.customMarker("somethingCutom")
				.option(StandardQueryOptions.DEBUG)
				.option(StandardQueryOptions.EXPLAIN)
				.build();

		step.getCache().put("k", "v");

		CubeQueryStep copy = CubeQueryStep.edit(step).build();

		// Check .equals, even if some fields are not in the equals
		Assertions.assertThat(copy).isEqualTo(step);

		// Check fields not in equals
		Assertions.assertThat(copy.isExplain()).isEqualTo(step.isExplain());
		Assertions.assertThat(copy.isDebug()).isEqualTo(step.isDebug());

		// Check Cache is not copied
		Assertions.assertThat(copy.getCache()).isEmpty();
		Assertions.assertThat(step.getCache()).hasSize(1);
	}

	@Test
	public void testEdit_withTransverseCache() {
		CubeQueryStep step = CubeQueryStep.builder()
				// This test should customize all fields
				.measure(Mockito.mock(IMeasure.class))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
				.customMarker("somethingCutom")
				.option(StandardQueryOptions.DEBUG)
				.option(StandardQueryOptions.EXPLAIN)
				.build();

		Map<Object, Object> transverseCache = new HashMap<>();
		step.setCrossStepsCache(transverseCache);

		step.getCache().put("k", "v");
		transverseCache.put("k2", "v2");

		CubeQueryStep copy = CubeQueryStep.edit(step).build();

		// Check .equals, even if some fields are not in the equals
		Assertions.assertThat(copy).isEqualTo(step);

		// Check fields not in equals
		Assertions.assertThat(copy.isExplain()).isEqualTo(step.isExplain());
		Assertions.assertThat(copy.isDebug()).isEqualTo(step.isDebug());

		// Check Cache is not copied
		Assertions.assertThat(copy.getCache()).containsKey("adhoc-transverseCache");
		Assertions.assertThat(step.getCache()).hasSize(2).containsKeys("k", "adhoc-transverseCache");
	}

	@Test
	public void testDebug() {
		CubeQueryStep stepNotDebug = CubeQueryStep.builder()
				.measure(Mockito.mock(IMeasure.class))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
				.build();
		Assertions.assertThat(stepNotDebug.isDebug()).isFalse();

		CubeQueryStep stepDebug = CubeQueryStep.edit(stepNotDebug).option(StandardQueryOptions.DEBUG).build();
		Assertions.assertThat(stepDebug.isDebug()).isTrue();

		Assertions.assertThat(stepDebug).isNotEqualTo(stepNotDebug);
	}

	@Test
	public void testCustomMarker() {
		CubeQueryStep stepHasCustom = CubeQueryStep.builder()
				.measure(Mockito.mock(IMeasure.class))
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(IGroupBy.GRAND_TOTAL)
				.customMarker(Optional.of("someCustomMarker"))
				.build();
		Assertions.assertThat((Optional) stepHasCustom.optCustomMarker()).contains("someCustomMarker");

		CubeQueryStep editKeepCustom = CubeQueryStep.edit(stepHasCustom).build();
		Assertions.assertThat((Optional) editKeepCustom.optCustomMarker()).contains("someCustomMarker");
	}
}
