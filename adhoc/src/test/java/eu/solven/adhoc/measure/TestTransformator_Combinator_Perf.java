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
package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.math.LongMath;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;

public class TestTransformator_Combinator_Perf extends ADagTest implements IAdhocTestConstants {
	int maxCardinality = 100_000;

	@BeforeEach
	@Override
	public void feedTable() {
		for (int i = 0; i < maxCardinality; i++) {
			rows.add(Map.of("l", "A", "row_index", i, "k1", i));
		}
	}

	@Test
	public void testSumOfSum() {
		amb.addMeasure(k1Sum);

		String timesNMinus1 = k1Sum.getName();
		String timesN = k1Sum.getName();

		// SUM(0..N) = N * (N-1)
		long sum = LongMath.checkedMultiply(maxCardinality, maxCardinality - 1);

		int height = 2;
		for (int i = 0; i < height; i++) {
			if (i == 0) {
				timesNMinus1 = k1Sum.getName();
			} else {
				timesNMinus1 = k1Sum.getName() + "x" + (1 << i);
			}
			timesN = k1Sum.getName() + "x" + (1 << (i + 1));

			amb.addMeasure(Combinator.builder()
					.name(timesN)
					.underlyings(Arrays.asList(timesNMinus1, timesNMinus1))
					.combinationKey(SumCombination.KEY)
					.build());
		}
		ITabularView output = aqw.execute(AdhocQuery.builder().measure(timesN
		// k1Sum.getName() + "x" + (1 << height)
		).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(timesN, 0L + sum * height));
	}
}
