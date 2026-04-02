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
package eu.solven.adhoc.encoding.dictionary;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.column.ObjectArrayImmutableColumn;

public class TestDistinctFreezer {
	@Test
	public void distinct16k_size16k() {
		List<String> list = IntStream.range(0, 16 * 1024).mapToObj(i -> "s_" + i).toList();

		long capped = DistinctFreezer.cappedDistinctCount(list, 1024);
		Assertions.assertThat(capped).isEqualTo(1024 + 1);

		long hll = DistinctFreezer.estimateDistinctHLL(list);
		Assertions.assertThat(hll).isEqualTo(16_062L);

		long kvm = DistinctFreezer.estimateDistinctKMV(list);
		Assertions.assertThat(kvm).isEqualTo(15_970L);
	}

	@Test
	public void distinct16_size16k() {
		List<String> list = IntStream.range(0, 16 * 1).map(i -> i % 16).mapToObj(i -> "s_" + i).toList();

		long capped = DistinctFreezer.cappedDistinctCount(list, 1);
		Assertions.assertThat(capped).isEqualTo(1 + 1);

		long hll = DistinctFreezer.estimateDistinctHLL(list);
		Assertions.assertThat(hll).isEqualTo(16);

		long kvm = DistinctFreezer.estimateDistinctKMV(list);
		Assertions.assertThat(kvm).isEqualTo(16);
	}

	@Test
	public void distinct16_size16() {
		List<String> list = IntStream.range(0, 16).map(i -> i).mapToObj(i -> "s_" + i).toList();

		long capped = DistinctFreezer.cappedDistinctCount(list, 1);
		Assertions.assertThat(capped).isEqualTo(1 + 1);

		long hll = DistinctFreezer.estimateDistinctHLL(list);
		Assertions.assertThat(hll).isEqualTo(16);

		long kvm = DistinctFreezer.estimateDistinctKMV(list);
		Assertions.assertThat(kvm).isEqualTo(16);
	}

	@Test
	public void distinctLocalDate() {
		List<LocalDate> list = IntStream.range(0, 16).mapToObj(i -> LocalDate.of(2020 + i, 1, 1)).toList();

		long capped = DistinctFreezer.cappedDistinctCount(list, 1);
		Assertions.assertThat(capped).isEqualTo(1 + 1);

		// long hll = DistinctFreezer.estimateDistinctHLL(list);
		// Assertions.assertThat(hll).isEqualTo(16_062L);
		//
		// long kvm = DistinctFreezer.estimateDistinctKMV(list);
		// Assertions.assertThat(kvm).isEqualTo(15_970L);

		new DistinctFreezer().freeze(ObjectArrayImmutableColumn.builder().asArray(list).build(), new HashMap<>());
	}

}
