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
package eu.solven.adhoc.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;

/**
 * Project-wide pinning tests for the interaction between JSpecify and Lombok's code generators. Lives in
 * {@code adhoc-model} so any module can rely on the conclusions.
 *
 * <p>
 * Bottom line for callers:
 * <ul>
 * <li><b>Package-level {@code @NullMarked} alone</b> is static-analysis only (NullAway, IDE highlights) — Lombok's
 * {@code @Builder} and {@code @SuperBuilder} do <b>not</b> generate a runtime null-check from package-level marking.
 * <a href="https://github.com/projectlombok/lombok/issues/3861">lombok#3861</a> tracks lifting this gap.</li>
 * <li><b>Field-level {@code @org.jspecify.annotations.NonNull}</b> <em>is</em> honoured by Lombok at runtime —
 * {@code .build()} throws {@code IllegalArgumentException} on null, exactly the same shape as {@code @lombok.NonNull}.
 * This is the empirical justification for picking {@code org.jspecify.annotations.NonNull} as the dominant
 * {@code NonNull} simple-name in {@code scripts/import-uniqueness.allow}: existing {@code @lombok.NonNull} usages can
 * be migrated to JSpecify with no behavioural change.</li>
 * <li><b>Field-level {@code @lombok.NonNull}</b> still works (legacy code path). Migration is opportunistic; new code
 * should use JSpecify.</li>
 * </ul>
 *
 * <p>
 * If the package-level tests start failing (i.e. {@code .build()} throws where it currently returns null), Lombok has
 * finally lifted the package-level gap and we can drop the field-level annotation entirely on classes whose package is
 * {@code @NullMarked}.
 *
 * <p>
 * The fixtures intentionally live in {@code eu.solven.adhoc.util} (which is {@code @NullMarked} via the production
 * {@code package-info.java} — src/main and src/test share the same package). The presence/absence of the field-level
 * annotation is the load-bearing detail; everything else is boilerplate.
 */
public class TestLombokJSpecify {

	/**
	 * @Builder + un-annotated field in a @NullMarked package: build() succeeds with the field == null.
	 */
	@Test
	public void testBuilder_packageNullMarked_alone_doesNotEnforceAtRuntime() {
		BuilderFixture built = BuilderFixture.builder().build();

		Assertions.assertThat(built).isNotNull();
		Assertions.assertThat(built.getName()).isNull();
	}

	/**
	 * @Builder + explicit @lombok.NonNull: build() throws (configured to {@code IllegalArgumentException} via
	 *          {@code lombok.config: lombok.nonNull.exceptionType = IllegalArgumentException}).
	 */
	@Test
	public void testBuilder_lombokNonNull_doesEnforceAtRuntime() {
		Assertions.assertThatThrownBy(() -> BuilderWithLombokNonNullFixture.builder().build())
				.isInstanceOf(IllegalArgumentException.class);
	}

	/**
	 * @SuperBuilder + un-annotated field in a @NullMarked package: same gap as @Builder. Pinned separately because
	 * @SuperBuilder is a different code generator and could in principle behave differently from @Builder.
	 */
	@Test
	public void testSuperBuilder_packageNullMarked_alone_doesNotEnforceAtRuntime() {
		SuperBuilderFixture built = SuperBuilderFixture.builder().build();

		Assertions.assertThat(built).isNotNull();
		Assertions.assertThat(built.getName()).isNull();
	}

	/**
	 * @SuperBuilder + explicit @lombok.NonNull: build() throws.
	 */
	@Test
	public void testSuperBuilder_lombokNonNull_doesEnforceAtRuntime() {
		Assertions.assertThatThrownBy(() -> SuperBuilderWithLombokNonNullFixture.builder().build())
				.isInstanceOf(IllegalArgumentException.class);
	}

	/**
	 * @Builder + explicit {@link org.jspecify.annotations.NonNull}: build() throws — same runtime contract as
	 *          {@code @lombok.NonNull}. This is the load-bearing test for the project-wide migration plan: the
	 *          {@code NonNull} simple-name canonical pick in {@code scripts/import-uniqueness.allow} is
	 *          {@code org.jspecify.annotations.NonNull}, and this test proves the swap is behaviour-preserving on
	 *          required builder fields.
	 */
	@Test
	public void testBuilder_jspecifyNonNull_doesEnforceAtRuntime() {
		Assertions.assertThatThrownBy(() -> BuilderWithJSpecifyNonNullFixture.builder().build())
				.isInstanceOf(IllegalArgumentException.class);
	}

	/**
	 * @SuperBuilder counterpart of {@link #testBuilder_jspecifyNonNull_doesEnforceAtRuntime()}. Pinned separately
	 *               because @SuperBuilder is a different code generator and could in principle drop JSpecify support
	 *               independently.
	 */
	@Test
	public void testSuperBuilder_jspecifyNonNull_doesEnforceAtRuntime() {
		Assertions.assertThatThrownBy(() -> SuperBuilderWithJSpecifyNonNullFixture.builder().build())
				.isInstanceOf(IllegalArgumentException.class);
	}

	// ── Fixtures ─────────────────────────────────────────────────────────────

	@Value
	@Builder
	public static class BuilderFixture {
		String name;
	}

	@Value
	@Builder
	public static class BuilderWithLombokNonNullFixture {
		@NonNull
		String name;
	}

	@Value
	@Builder
	public static class BuilderWithJSpecifyNonNullFixture {
		@org.jspecify.annotations.NonNull
		String name;
	}

	@Value
	@SuperBuilder
	public static class SuperBuilderFixture {
		String name;
	}

	@Value
	@SuperBuilder
	public static class SuperBuilderWithLombokNonNullFixture {
		@NonNull
		String name;
	}

	@Value
	@SuperBuilder
	public static class SuperBuilderWithJSpecifyNonNullFixture {
		@org.jspecify.annotations.NonNull
		String name;
	}
}
