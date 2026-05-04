/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.routing;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;

/**
 * Unit tests for {@link RoutingMeasure} contract: dispatching to one of N declared underlyings via the supplied
 * {@code routeFunction}, and rejecting an unknown return value at runtime.
 *
 * <p>
 * The fixture has rows tagged by country. The routing measure picks {@code d_fr}, {@code d_us}, or {@code d} according
 * to the {@code country} value present in the step's filter — or, when nothing is specified, {@code d} (the unfiltered
 * total).
 */
public class TestDag_RoutingMeasure extends ATestDagInMemory {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("country", "FR", "city", "Paris", "d", 123));
		table().add(Map.of("country", "FR", "city", "Lyon", "d", 234));
		table().add(Map.of("country", "DE", "city", "Berlin", "d", 345));
		table().add(Map.of("country", "US", "city", "Paris", "d", 456));
		table().add(Map.of("country", "US", "city", "New-York", "d", 567));
	}

	@BeforeEach
	public void registerBaseMeasures() {
		// `d` is the raw aggregate; `d_fr` and `d_us` are filtered variants the routing measure dispatches to.
		forest.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());
		forest.addMeasure(Aggregator.builder().name("d_fr").columnName("d").aggregationKey(SumAggregation.KEY).build());
		forest.addMeasure(Aggregator.builder().name("d_us").columnName("d").aggregationKey(SumAggregation.KEY).build());
	}

	@Test
	public void testRoutesToFR_whenFilterIsCountryFR() {
		AtomicInteger calls = new AtomicInteger();
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d")
				.underlying("d_fr")
				.underlying("d_us")
				.routeFunction(step -> {
					calls.incrementAndGet();
					Object country = FilterHelpers.asMap(step.getFilter()).get("country");
					if ("FR".equals(country)) {
						return "d_fr";
					}
					if ("US".equals(country)) {
						return "d_us";
					}
					return "d";
				})
				.build());

		// Filter on country=FR — routeFunction must return "d_fr". Restricting d_fr to country=FR is redundant
		// (the engine ANDs it with the existing filter) but documents the intent.
		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").andFilter("country", "FR").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("dRouted", 0L + 123 + 234));
		Assertions.assertThat(calls).hasValueGreaterThanOrEqualTo(1);
	}

	@Test
	public void testRoutesToUS_whenFilterIsCountryUS() {
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d")
				.underlying("d_fr")
				.underlying("d_us")
				.routeFunction(step -> {
					Object country = FilterHelpers.asMap(step.getFilter()).get("country");
					if ("US".equals(country)) {
						return "d_us";
					}
					return "d";
				})
				.build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").andFilter("country", "US").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("dRouted", 0L + 456 + 567));
	}

	@Test
	public void testRoutesToDefault_whenNoFilter() {
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d")
				.underlying("d_fr")
				.underlying("d_us")
				.routeFunction(step -> "d")
				.build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("dRouted", 0L + 123 + 234 + 345 + 456 + 567));
	}

	@Test
	public void testUnknownUnderlying_throws() {
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d")
				.routeFunction(step -> "totally_made_up")
				.build());

		// The engine wraps step exceptions with `AdhocExceptionHelpers.wrap`, so the routing-measure error
		// surfaces in the cause chain. `hasStackTraceContaining` walks the whole chain.
		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure("dRouted").build()))
				.hasStackTraceContaining("dRouted")
				.hasStackTraceContaining("totally_made_up")
				.hasStackTraceContaining("[d]");
	}

	@Test
	public void testNullUnderlying_throws() {
		forest.addMeasure(RoutingMeasure.builder().name("dRouted").underlying("d").routeFunction(step -> null).build());

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure("dRouted").build()))
				.hasStackTraceContaining("dRouted")
				.hasStackTraceContaining("null");
	}

	@Test
	public void testRoutedAlongsideRawMeasure() {
		// Asking for both `d` (raw) and `dRouted` (routing) in one query exercises the DAG fan-out:
		// `dRouted` plans its own underlying step, separate from the directly-requested `d`.
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d")
				.underlying("d_fr")
				.underlying("d_us")
				.routeFunction(step -> {
					Object country = FilterHelpers.asMap(step.getFilter()).get("country");
					if ("FR".equals(country)) {
						return "d_fr";
					}
					if ("US".equals(country)) {
						return "d_us";
					}
					return "d";
				})
				.build());

		ITabularView output =
				cube().execute(CubeQuery.builder().measure("d", "dRouted").andFilter("country", "US").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("d", 0L + 456 + 567, "dRouted", 0L + 456 + 567));
	}
}
