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
package eu.solven.adhoc.query.filter.stripper;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;

public class TestFilterStripper {
	@Test
	public void testSharedCache() {
		FilterStripper stripper = FilterStripper.builder().where(AndFilter.and(Map.of("c", "c1", "d", "d2"))).build();
		Assertions.assertThat(stripper.filterToStripper.asMap()).isEmpty();

		Assertions.assertThat(stripper.isStricterThan(ColumnFilter.matchEq("c", "c1"))).isTrue();
		Assertions.assertThat(stripper.filterToStripper.asMap()).hasSize(1);

		FilterStripper relatedStripper = stripper.withWhere(ColumnFilter.matchEq("e", "e3"));
		// Ensure the cache unrelated to current WHERE is shared
		Assertions.assertThat(relatedStripper.filterToStripper.asMap()).hasSize(1);
		// Ensure the cache related to current WHERE is not-shared
		Assertions.assertThat(relatedStripper.knownAsStricter.asMap()).isEmpty();
	}
}
