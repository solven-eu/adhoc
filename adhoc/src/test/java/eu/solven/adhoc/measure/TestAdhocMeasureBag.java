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

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.model.Aggregator;

public class TestAdhocMeasureBag {

	Aggregator inital = Aggregator.builder().name("someName").columnName("someColumn").build();
	Aggregator later = Aggregator.builder().name("someName").columnName("otherColumn").build();

	@Test
	public void testReplaceMeasure() {
		UnsafeAdhocMeasureBag measureBag = UnsafeAdhocMeasureBag.builder().name("testReplaceMeasure").build();
		measureBag.addMeasure(inital);
		measureBag.addMeasure(later);

		Assertions.assertThat(measureBag.getNameToMeasure()).hasSize(1).containsEntry("someName", later);
	}

	@Test
	public void testFromList() {
		Assertions
				.assertThatThrownBy(
						() -> AdhocMeasureBag.fromMeasures("testReplaceMeasure", Arrays.asList(inital, later)))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
