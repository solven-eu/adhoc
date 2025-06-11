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

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestCombinator {
	@Test
	public void testOptions_bucketor() {
		Partitionor measure = Partitionor.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.groupBy(GroupByColumns.named("c"))
				.build();
		Map<String, ?> allOptions = Combinator.makeAllOptions(measure, Map.of("k2", "v2"));

		Assertions.assertThat(Map.<String, Object>copyOf(allOptions))
				.hasSize(4)
				.containsEntry("k2", "v2")
				.containsEntry("underlyingNames", List.of())
				// .containsEntry("groupByColumns", Set.of("c"))
				.containsEntry("measure", measure);

		// This checks there is no StackOverFlow on .toString, which is possible as we may set `measure==this` in
		// `.makeAllOptions`
		Assertions.assertThat(measure.toString()).contains("measure");
	}

	@Test
	public void testWithTags() {
		Combinator measure = Combinator.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.combinationKey("someKey")
				.tag("tag1")
				.build();

		Assertions.assertThat(measure.withTags(ImmutableSet.of("tag2"))).satisfies(edited -> {
			Assertions.assertThat(edited.getTags()).containsExactly("tag2");
		});
	}

	@Test
	public void testAddOptions() {
		Combinator measure = Combinator.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.combinationKey("someKey")
				.tag("tag1")
				.build();

		Combinator moreOptions = measure.toBuilder().combinationOption("k2", "v2").build();

		Assertions.assertThat(measure.getCombinationOptions()).isEqualTo(Map.of("k", "v"));
		Assertions.assertThat(moreOptions.getCombinationOptions()).isEqualTo(Map.of("k", "v", "k2", "v2"));
	}
}
