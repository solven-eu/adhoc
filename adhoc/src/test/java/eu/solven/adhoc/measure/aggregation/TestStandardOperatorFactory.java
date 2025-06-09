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
package eu.solven.adhoc.measure.aggregation;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.SumAggregation;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class TestStandardOperatorFactory {
	StandardOperatorFactory factory = new StandardOperatorFactory();

	@Test
	public void testAggregation_byKey() {
		IAggregation aggregation = factory.makeAggregation(SumAggregation.KEY, Map.of());

		Assertions.assertThat(aggregation).isInstanceOf(SumAggregation.class);
	}

	@Test
	public void testAggregation_byClassQualifiedName() {
		IAggregation aggregation = factory.makeAggregation(CustomAggregation.class.getName());

		Assertions.assertThat(aggregation).isInstanceOf(CustomAggregation.class);
	}

	@Test
	public void testAggregation_unknownKey() {
		Assertions.assertThatThrownBy(() -> factory.makeAggregation("someUnknownKey"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("someUnknownKey");
	}

	@RequiredArgsConstructor
	public static class WithOptionsCombination implements ICombination {
		@Getter
		final Map<String, ?> options;
	}

	@Test
	public void testOptions() {
		ICombination combination = factory.makeCombination(WithOptionsCombination.class.getName(), Map.of("k", "v"));

		Assertions.assertThat(combination).isInstanceOfSatisfying(WithOptionsCombination.class, withOptions -> {
			Assertions.assertThat((Map) withOptions.getOptions()).containsEntry("k", "v");
			Assertions.assertThat((Map) withOptions.getOptions()).containsEntry("operatorFactory", factory);
		});
	}
}
