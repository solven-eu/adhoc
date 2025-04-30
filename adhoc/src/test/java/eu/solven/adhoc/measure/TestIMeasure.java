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
package eu.solven.adhoc.measure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.SumCombination;

public class TestIMeasure {
	@Test
	public void testSum() {
		Assertions.assertThat(IMeasure.sum("sum", "a")).isEqualTo(IMeasure.alias("sum", "a"));
		Assertions.assertThat(IMeasure.sum("sum", "a", "b")).isInstanceOfSatisfying(Combinator.class, m -> {
			Assertions.assertThat(m.getName()).isEqualTo("sum");
			Assertions.assertThat(m.getCombinationKey()).isEqualTo(SumCombination.KEY);
			Assertions.assertThat(m.getUnderlyingNames()).containsExactly("a", "b");
		});
	}

	@Test
	public void testAlias() {
		Assertions.assertThat(IMeasure.alias("someMeasure", "someMeasure"))
				.isEqualTo(ReferencedMeasure.ref("someMeasure"));
		Assertions.assertThat(IMeasure.alias("someMeasure", "otherMeasure"))
				.isInstanceOfSatisfying(Combinator.class, m -> {
					Assertions.assertThat(m.getName()).isEqualTo("someMeasure");
					Assertions.assertThat(m.getCombinationKey()).isEqualTo(FindFirstCombination.KEY);
					Assertions.assertThat(m.getUnderlyingNames()).containsExactly("otherMeasure");
				});
	}
}
