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
package eu.solven.adhoc.engine.tabular.splitter.merger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Unit-tests for {@link MergeInducersIntoSingle#buildGroupByCoverageString} in isolation, without touching the logging
 * framework.
 */
public class TestMergeInducersIntoSingleCoverageString {

	MergeInducersIntoSingle merger = MergeInducersIntoSingle.builder().build();

	TableQueryStep a = TableQueryStep.builder().aggregator(Aggregator.sum("k")).build();

	/**
	 * Two steps both group by {@code country} (100 % coverage), only one also groups by {@code currency} (50 %
	 * coverage). {@code region} was added to the merged groupBy solely to support row-splitting (0 % native coverage).
	 */
	@Test
	public void coverageString_100and50and0percent() {
		// s1 groups by country+currency, restricted to region==EU
		TableQueryStep s1 = a.toBuilder()
				.groupBy(GroupByColumns.named("country", "currency"))
				.filter(ColumnFilter.matchEq("region", "EU"))
				.build();
		// s2 groups by country only, restricted to region==US
		TableQueryStep s2 = a.toBuilder()
				.groupBy(GroupByColumns.named("country"))
				.filter(ColumnFilter.matchEq("region", "US"))
				.build();

		IGroupBy mergedGroupBy = GroupByColumns.named("country", "currency", "region");

		String coverage = merger.buildGroupByCoverageString(ImmutableSet.of(s1, s2), mergedGroupBy);

		// country was in both steps' groupBy → 100 %
		Assertions.assertThat(coverage).contains("country=100%(2/2)");
		// currency was only in s1's groupBy → 50 %
		Assertions.assertThat(coverage).contains("currency=50%(1/2)");
		// region was in neither step's groupBy (added for splitting) → 0 %
		Assertions.assertThat(coverage).contains("region=0%(0/2)");
	}
}
