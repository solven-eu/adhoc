/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dag;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.transformers.Aggregator;

public class TestAdhocQueryStep {
	@Test
	public void testDebug() {
		AdhocQueryStep stepNotDebug = AdhocQueryStep.builder()
				.measure(Aggregator.sum("c"))
				.filter(IAdhocFilter.MATCH_ALL)
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.build();
		Assertions.assertThat(stepNotDebug.isDebug()).isFalse();

		AdhocQueryStep stepDebug = AdhocQueryStep.edit(stepNotDebug).debug(true).build();
		Assertions.assertThat(stepDebug.isDebug()).isTrue();

		Assertions.assertThat(stepDebug).isEqualTo(stepNotDebug);
	}

	@Test
	public void testCustomMarker() {
		AdhocQueryStep stepHasCustom = AdhocQueryStep.builder()
				.measure(Aggregator.sum("c"))
				.filter(IAdhocFilter.MATCH_ALL)
				.groupBy(IAdhocGroupBy.GRAND_TOTAL)
				.customMarker(Optional.of("someCustomMarker"))
				.build();
		Assertions.assertThat((Optional) stepHasCustom.getCustomMarker()).contains("someCustomMarker");

		AdhocQueryStep editKeepCustom = AdhocQueryStep.edit(stepHasCustom).build();
		Assertions.assertThat((Optional) editKeepCustom.getCustomMarker()).contains("someCustomMarker");
	}
}
