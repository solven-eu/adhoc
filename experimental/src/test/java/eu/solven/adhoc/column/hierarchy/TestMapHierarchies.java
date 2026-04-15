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
package eu.solven.adhoc.column.hierarchy;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

public class TestMapHierarchies {

	@Test
	public void testGetParent_topLevel_returnsRoot() {
		MapHierarchies hierarchies = MapHierarchies.builder()
				.hierarchyToColumn("geo", ImmutableList.of("country", "region", "city"))
				.build();

		Assertions.assertThat(hierarchies.getParent("country")).contains(IHierarchies.ROOT);
	}

	@Test
	public void testGetParent_intermediate_returnsPrevious() {
		MapHierarchies hierarchies = MapHierarchies.builder()
				.hierarchyToColumn("geo", ImmutableList.of("country", "region", "city"))
				.build();

		Assertions.assertThat(hierarchies.getParent("region")).contains("country");
		Assertions.assertThat(hierarchies.getParent("city")).contains("region");
	}

	@Test
	public void testGetParent_unknownColumn_returnsEmpty() {
		MapHierarchies hierarchies = MapHierarchies.builder()
				.hierarchyToColumn("geo", ImmutableList.of("country", "region", "city"))
				.build();

		Assertions.assertThat(hierarchies.getParent("unknown")).isEmpty();
	}

	@Test
	public void testGetParent_multipleHierarchies() {
		MapHierarchies hierarchies = MapHierarchies.builder()
				.hierarchyToColumn("geo", ImmutableList.of("country", "region", "city"))
				.hierarchyToColumn("time", ImmutableList.of("year", "month", "day"))
				.build();

		Assertions.assertThat(hierarchies.getParent("country")).contains(IHierarchies.ROOT);
		Assertions.assertThat(hierarchies.getParent("year")).contains(IHierarchies.ROOT);
		Assertions.assertThat(hierarchies.getParent("day")).contains("month");
	}

	@Test
	public void testGetParent_emptyHierarchies_returnsEmpty() {
		MapHierarchies hierarchies = MapHierarchies.builder().build();

		Assertions.assertThat(hierarchies.getParent("anything")).isEmpty();
	}

	@Test
	public void testGetParent_singleElementHierarchy_returnsRoot() {
		MapHierarchies hierarchies =
				MapHierarchies.builder().hierarchyToColumn("mono", ImmutableList.of("only")).build();

		Assertions.assertThat(hierarchies.getParent("only")).contains(IHierarchies.ROOT);
	}

	@Test
	public void testGetParent_columnPresentInTwoHierarchies_returnsFirstMatch() {
		MapHierarchies hierarchies = MapHierarchies.builder()
				.hierarchyToColumn("h1", ImmutableList.of("a", "shared"))
				.hierarchyToColumn("h2", ImmutableList.of("shared", "b"))
				.build();

		Optional<String> parent = hierarchies.getParent("shared");
		Assertions.assertThat(parent).isPresent();
		Assertions.assertThat(parent.get()).isIn("a", IHierarchies.ROOT);
	}
}
