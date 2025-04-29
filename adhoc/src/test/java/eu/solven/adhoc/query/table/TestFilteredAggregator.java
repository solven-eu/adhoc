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
package eu.solven.adhoc.query.table;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestFilteredAggregator {
	final Aggregator a = Aggregator.countAsterisk();

	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(FilteredAggregator.class).verify();
	}

	@Test
	public void testAliasIndexDefault() {
		FilteredAggregator aggregator =
				FilteredAggregator.builder().aggregator(a).filter(IAdhocFilter.MATCH_ALL).build();

		Assertions.assertThat(aggregator.getAlias()).isEqualTo(a.getName());
	}

	@Test
	public void testAliasIndex0() {
		FilteredAggregator aggregator =
				FilteredAggregator.builder().aggregator(a).filter(IAdhocFilter.MATCH_ALL).index(0).build();

		Assertions.assertThat(aggregator.getAlias()).isEqualTo(a.getName());
	}

	@Test
	public void testAliasIndex1() {
		FilteredAggregator aggregator =
				FilteredAggregator.builder().aggregator(a).filter(IAdhocFilter.MATCH_ALL).index(1).build();

		Assertions.assertThat(aggregator.getAlias()).isEqualTo(a.getName() + "_1");
	}
}
