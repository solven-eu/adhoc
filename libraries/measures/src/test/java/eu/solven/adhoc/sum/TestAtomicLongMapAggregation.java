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
package eu.solven.adhoc.sum;

import java.time.LocalDate;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.measure.aggregation.collection.AtomicLongMapAggregation;

public class TestAtomicLongMapAggregation {
	AtomicLongMapAggregation agg = new AtomicLongMapAggregation();

	@Test
	public void testAggregation() {
		AtomicLongMap<String> k1 = AtomicLongMap.create();
		k1.addAndGet("k1", 123);

		Assertions.assertThat(agg.aggregate((Object) null, null)).isEqualTo(null);

		Assertions.assertThat(agg.aggregate(AtomicLongMap.create(), null)).isEqualTo(null);
		Assertions.assertThat(agg.aggregate(null, k1)).isEqualTo(k1);

		AtomicLongMap<String> k2 = AtomicLongMap.create();
		k2.addAndGet("k2", 234);
		Assertions.assertThat(agg.aggregate(k1, k2).asMap()).isEqualTo(Map.of("k1", 123L, "k2", 234L));

		AtomicLongMap<String> k2k3 = AtomicLongMap.create();
		k2k3.addAndGet("k2", 345);
		k2k3.addAndGet("k3", 456);
		Assertions.assertThat(agg.aggregate(k2, k2k3).asMap()).isEqualTo(Map.of("k2", 234L + 345L, "k3", 456L));

		LocalDate now = LocalDate.now();
		AtomicLongMap<LocalDate> nowMap = AtomicLongMap.create();
		nowMap.addAndGet(now, 567);
		Assertions.assertThat(agg.aggregate(k1, nowMap).asMap()).isEqualTo(Map.of("k1", 123L, now, 567L));

	}
}
