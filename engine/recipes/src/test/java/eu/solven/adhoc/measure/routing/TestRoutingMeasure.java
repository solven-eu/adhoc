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

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.pepper.unittest.PepperJackson3TestHelper;

/**
 * Lightweight unit tests for {@link RoutingMeasure} basic operations: builder defaults, {@code toString}, Jackson
 * round-trip. Intentionally does NOT extend a cube fixture — these tests only touch the spec class. Full end-to-end
 * behaviour (DAG fan-out, coalesce semantics, error paths) lives in {@link TestDag_RoutingMeasure}.
 *
 * <p>
 * Round-trip is performed against {@link RoutingMeasure}{@code .class} as the static type (no polymorphic
 * discriminator). Serializing through {@code IMeasure} would require registering {@link RoutingMeasure} as a
 * {@code @JsonSubTypes} entry on {@code IMeasure} so Jackson's {@code MINIMAL_CLASS} resolution can find this class
 * across the module boundary — that's a forest-wide concern, not specific to this measure, and is out of scope here.
 */
public class TestRoutingMeasure {

	@Test
	public void testBuilderDefaults() {
		RoutingMeasure m = RoutingMeasure.builder().name("dRouted").underlying("d_legacy").build();

		Assertions.assertThat(m.getName()).isEqualTo("dRouted");
		Assertions.assertThat(m.getUnderlyings()).containsExactly("d_legacy");
		Assertions.assertThat(m.getUnderlyingNames()).containsExactly("d_legacy");
		Assertions.assertThat(m.getTags()).isEmpty();
		Assertions.assertThat(m.getRoutingOptions()).isEmpty();
		Assertions.assertThat(m.getRoutingLogic()).isNull();
		Assertions.assertThat(m.queryStepClass()).isEqualTo(RoutingMeasureQueryStep.class.getName());
	}

	@Test
	public void testToString_containsNameAndUnderlyings() {
		RoutingMeasure m = RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.routingLogic(step -> List.of())
				.build();

		Assertions.assertThat(m.toString()).contains("dRouted").contains("d_legacy").contains("d_modern");
	}

	/**
	 * Round-trips a spec-only RoutingMeasure (no routingLogic). The deserialized instance is usable for introspection —
	 * {@link RoutingMeasure} excludes {@code routingLogic} from equality, so the round-trip is value-equal to the
	 * original.
	 */
	@Test
	public void testJackson_specOnly() {
		RoutingMeasure original = RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.tag("migration")
				.routingOptions(Map.of("cutoff", "2026-01-01"))
				.build();

		String asString = PepperJackson3TestHelper.verifyJackson(RoutingMeasure.class, original);

		Assertions.assertThat(asString)
				.contains("dRouted")
				.contains("d_legacy")
				.contains("d_modern")
				.contains("migration")
				.contains("2026-01-01")
				// `routingLogic` must not appear — it is @JsonIgnore (lambdas / interface refs are not serializable).
				.doesNotContain("routingLogic");
	}

	/**
	 * A populated routingLogic survives serialization (it is dropped) and deserialization (the new instance has
	 * routingLogic == null), and the spec equality holds because routingLogic is excluded from equality.
	 */
	@Test
	public void testJackson_routingLogicDroppedOnRoundtrip() {
		RoutingMeasure original =
				RoutingMeasure.builder().name("dRouted").underlying("d_legacy").routingLogic(_ -> List.of()).build();

		String asString = PepperJackson3TestHelper.verifyJackson(RoutingMeasure.class, original);

		Assertions.assertThat(asString).doesNotContain("routingLogic");
	}

	// The pinning test for "Lombok does NOT consult @NullMarked at build time" lives in adhoc-model's
	// `eu.solven.adhoc.util.TestLombokJSpecify` — it covers @Builder and @SuperBuilder with and without
	// @lombok.NonNull, and is the project-wide reference for the `@lombok.NonNull` requirement on required
	// builder fields. See https://github.com/projectlombok/lombok/issues/3861 and CONVENTIONS.MD.
}
