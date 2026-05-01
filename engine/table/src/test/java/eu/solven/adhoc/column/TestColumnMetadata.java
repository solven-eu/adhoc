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
package eu.solven.adhoc.column;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumnMetadata {

	@Test
	public void testBuilder_defaultsTypeToObject() {
		// `type` has @Default = Object.class — useful for tests/edge cases that build a column purely by name.
		ColumnMetadata md = ColumnMetadata.builder().name("c").build();

		Assertions.assertThat(md.getType()).isEqualTo(Object.class);
		Assertions.assertThat(md.getName()).isEqualTo("c");
		Assertions.assertThat(md.getTags()).isEmpty();
		Assertions.assertThat(md.getAliases()).isEmpty();
	}

	@Test
	public void testBuilder_capturesTagsAndAliases() {
		ColumnMetadata md = ColumnMetadata.builder()
				.name("c")
				.type(String.class)
				.tag("PII")
				.tag("public")
				.alias("col_c")
				.alias("c_v2")
				.build();

		Assertions.assertThat(md.getTags()).containsExactlyInAnyOrder("PII", "public");
		Assertions.assertThat(md.getAliases()).containsExactlyInAnyOrder("col_c", "c_v2");
	}

	@Test
	public void testMerge_emptyCollection_throws() {
		// Documented contract: merge requires at least one column.
		Assertions.assertThatThrownBy(() -> ColumnMetadata.merge(List.of()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("at least one");
	}

	@Test
	public void testMerge_singleColumn_returnsEquivalent() {
		ColumnMetadata only = ColumnMetadata.builder().name("c").type(Long.class).tag("t1").alias("c_v2").build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(only));

		Assertions.assertThat(merged.getName()).isEqualTo("c");
		Assertions.assertThat(merged.getType()).isEqualTo(Long.class);
		Assertions.assertThat(merged.getTags()).containsExactly("t1");
		Assertions.assertThat(merged.getAliases()).containsExactly("c_v2");
	}

	@Test
	public void testMerge_sameType_keepsType() {
		ColumnMetadata a = ColumnMetadata.builder().name("c").type(String.class).build();
		ColumnMetadata b = ColumnMetadata.builder().name("c").type(String.class).build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getType()).isEqualTo(String.class);
	}

	@Test
	public void testMerge_relatedTypes_fallsBackToCommonAncestor() {
		// Long and Double share Number as a non-trivial common ancestor — Spring's `determineCommonAncestor`
		// only picks ancestors above Object, so it returns Number here.
		ColumnMetadata a = ColumnMetadata.builder().name("c").type(Long.class).build();
		ColumnMetadata b = ColumnMetadata.builder().name("c").type(Double.class).build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getType())
				.isEqualTo(Number.class)
				.isAssignableFrom(Long.class)
				.isAssignableFrom(Double.class);
	}

	@Test
	public void testMerge_unrelatedTypes_fallsBackToObject() {
		ColumnMetadata a = ColumnMetadata.builder().name("c").type(String.class).build();
		ColumnMetadata b = ColumnMetadata.builder().name("c").type(Double.class).build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getType()).isEqualTo(Object.class);
	}

	@Test
	public void testMerge_aliases_keepIntersectionOnly() {
		// Composite-cube semantic: an alias declared by every sub-cube survives; an alias declared by only one is
		// dropped (so the composite doesn't claim aliases that are not universally valid).
		// Regression: `reduce` was seeded with `ImmutableSet.of()` which made `Sets.intersection(empty, anything)`
		// short-circuit to empty on every iteration — so a "shared" alias used to be silently dropped. Asserting
		// `containsExactly("shared")` here ensures the seed comes from the first element, not an empty set.
		ColumnMetadata a = ColumnMetadata.builder().name("c").alias("shared").alias("only-a").build();
		ColumnMetadata b = ColumnMetadata.builder().name("c").alias("shared").alias("only-b").build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getAliases()).containsExactly("shared");
	}

	@Test
	public void testMerge_tags_keepUnion() {
		// Composite-cube semantic: a tag declared by any sub-cube applies to the merged column (tags describe
		// data nature, not authorisation).
		ColumnMetadata a = ColumnMetadata.builder().name("c").tag("PII").tag("a-only").build();
		ColumnMetadata b = ColumnMetadata.builder().name("c").tag("PII").tag("b-only").build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getTags()).containsExactlyInAnyOrder("PII", "a-only", "b-only");
	}

	@Test
	public void testMerge_keepsNameOfFirst() {
		// `merge` is implemented by editing a copy of the first input — the resulting `name` is that of the first.
		ColumnMetadata a = ColumnMetadata.builder().name("first").build();
		ColumnMetadata b = ColumnMetadata.builder().name("second").build();

		ColumnMetadata merged = ColumnMetadata.merge(List.of(a, b));

		Assertions.assertThat(merged.getName()).isEqualTo("first");
	}

	@Test
	public void testToBuilder_roundtripPreservesEquality() {
		ColumnMetadata md = ColumnMetadata.builder().name("c").type(String.class).tag("t").alias("a").build();

		ColumnMetadata copy = md.toBuilder().build();

		Assertions.assertThat(copy).isEqualTo(md);
	}
}
