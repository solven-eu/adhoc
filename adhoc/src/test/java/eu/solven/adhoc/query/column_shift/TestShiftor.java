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
package eu.solven.adhoc.query.column_shift;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestShiftor {
	@Test
	public void testShiftAll() {
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
		Assertions.assertThat(Shiftor.shift("c", "v1", filter)).isEqualTo(ColumnFilter.isEqualTo("c", "v1"));
	}

	@Test
	public void testShiftColumn() {
		IAdhocFilter filter = ColumnFilter.isEqualTo("a", "a1");
		Assertions.assertThat(Shiftor.shift("c", "v1", filter)).isEqualTo(AndFilter.and(Map.of("a", "a1", "c", "v1")));
	}

	@Test
	public void testShiftIfPresent() {
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
		Assertions.assertThat(Shiftor.shiftIfPresent("c", "v1", filter)).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testMatchNone() {
		IAdhocFilter filter = IAdhocFilter.MATCH_NONE;
		Assertions.assertThat(Shiftor.shift("c", "v1", filter)).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testShiftAnd() {
		IAdhocFilter filter = AndFilter.and(Map.of("a", "a1", "b", "b1", "c", "c1"));
		Assertions.assertThat(Shiftor.shift("c", "c2", filter))
				.isEqualTo(AndFilter.and(Map.of("a", "a1", "b", "b1", "c", "c2")));
	}
}
