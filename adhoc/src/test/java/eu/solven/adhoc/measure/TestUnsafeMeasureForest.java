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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;

public class TestUnsafeMeasureForest implements IAdhocTestConstants {
	@Test
	public void testAddTag() {
		IMeasureForest baseForest = UnsafeMeasureForest.builder()
				.name("base")
				.measure(Aggregator.countAsterisk())
				.measure(filterK1onA1)
				.build();

		IMeasureForest updatedForest = baseForest.acceptVisitor(new IMeasureForestVisitor() {
			@Override
			public Set<IMeasure> mapMeasure(IMeasure measure) {
				if (measure.getName().equals(filterK1onA1.getName())) {
					return Set.of(((Filtrator) measure).toBuilder().tag("someTag").build());
				} else {
					return Set.of(measure);
				}
			}
		});

		Assertions.assertThat(updatedForest).isSameAs(baseForest);

		// Check the updated forest has been mutated
		Assertions.assertThat(updatedForest.getMeasures()).hasSize(2).anySatisfy(m -> {
			Assertions.assertThat(m.getName()).isEqualTo(filterK1onA1.getName());
			Assertions.assertThat(m.getTags()).containsExactly("someTag");
		});
	}
}
