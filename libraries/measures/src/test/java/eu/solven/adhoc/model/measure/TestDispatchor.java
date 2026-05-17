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
package eu.solven.adhoc.model.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link Dispatchor#toString()} — used in EXPLAIN logs, plan-tree labels, and exception messages. Same stability
 * surface as {@link TestFiltrator}: the conditional-skip on empty {@code tags} / {@code aggregationOptions} /
 * {@code decompositionOptions} is intentional and dropping it would drown meaningful fields in noise.
 *
 * @author Benoit Lacelle
 */
public class TestDispatchor {

	@Test
	public void testToString_nominal_omitsEmptyTagsAndOptions() {
		// All three optional fields default to empty — the toString should skip every one of them and render only
		// the four mandatory fields (name, underlying, aggregationKey, decompositionKey).
		Dispatchor dispatchor = Dispatchor.builder()
				.name("event_count")
				.underlying("events")
				.decompositionKey("eu.solven.adhoc.example.worldcup.DispatchedEvents")
				.build();

		Assertions.assertThat(dispatchor)
				.hasToString(
						"Dispatchor(name=event_count, underlying=events, aggregationKey=SUM, decompositionKey=eu.solven.adhoc.example.worldcup.DispatchedEvents)");
	}

	@Test
	public void testToString_defaultKeys_renderedExplicitly() {
		// Even at default values, both keys are rendered. That's deliberate — a developer reading EXPLAIN logs
		// needs to see which aggregation / decomposition is in play without cross-referencing the source.
		Dispatchor dispatchor = Dispatchor.builder().name("d").underlying("u").build();

		Assertions.assertThat(dispatchor)
				.hasToString("Dispatchor(name=d, underlying=u, aggregationKey=SUM, decompositionKey=identity)");
	}

	@Test
	public void testToString_withTags_rendersTagsBetweenNameAndUnderlying() {
		Dispatchor dispatchor = Dispatchor.builder().name("d").tag("debug").underlying("u").build();

		String s = dispatchor.toString();
		Assertions.assertThat(s)
				.startsWith("Dispatchor(name=d, tags=[")
				.contains("debug")
				.contains(", underlying=u")
				.contains(", aggregationKey=SUM")
				.contains(", decompositionKey=identity")
				.endsWith(")");
	}

	@Test
	public void testToString_withAggregationOptions_rendersAfterKey() {
		Dispatchor dispatchor = Dispatchor.builder().name("d").underlying("u").aggregationOption("k1", "v1").build();

		String s = dispatchor.toString();
		Assertions.assertThat(s)
				.contains(", aggregationKey=SUM, aggregationOptions={k1=v1}, decompositionKey=identity")
				.endsWith(")");
	}

	@Test
	public void testToString_withDecompositionOptions_rendersAfterKey() {
		Dispatchor dispatchor = Dispatchor.builder()
				.name("d")
				.underlying("u")
				.decompositionKey("custom-decomp")
				.decompositionOption("k1", "v1")
				.build();

		String s = dispatchor.toString();
		Assertions.assertThat(s).endsWith(", decompositionKey=custom-decomp, decompositionOptions={k1=v1})");
	}

	@Test
	public void testToString_fieldOrder_stable() {
		// Pin the exact field order: name → optional tags → underlying → aggregationKey → optional aggregationOptions
		// → decompositionKey → optional decompositionOptions. Tooling that greps EXPLAIN logs relies on this.
		Dispatchor dispatchor = Dispatchor.builder()
				.name("d")
				.tag("t")
				.underlying("u")
				.aggregationOption("ak", "av")
				.decompositionKey("dk")
				.decompositionOption("dko", "dvo")
				.build();

		String s = dispatchor.toString();
		int idxName = s.indexOf("name=");
		int idxTags = s.indexOf("tags=");
		int idxUnderlying = s.indexOf("underlying=");
		int idxAggKey = s.indexOf("aggregationKey=");
		int idxAggOpts = s.indexOf("aggregationOptions=");
		int idxDecKey = s.indexOf("decompositionKey=");
		int idxDecOpts = s.indexOf("decompositionOptions=");
		Assertions.assertThat(idxName).isGreaterThanOrEqualTo(0);
		Assertions.assertThat(idxTags).isGreaterThan(idxName);
		Assertions.assertThat(idxUnderlying).isGreaterThan(idxTags);
		Assertions.assertThat(idxAggKey).isGreaterThan(idxUnderlying);
		Assertions.assertThat(idxAggOpts).isGreaterThan(idxAggKey);
		Assertions.assertThat(idxDecKey).isGreaterThan(idxAggOpts);
		Assertions.assertThat(idxDecOpts).isGreaterThan(idxDecKey);
		Assertions.assertThat(s).endsWith(")");
	}
}
