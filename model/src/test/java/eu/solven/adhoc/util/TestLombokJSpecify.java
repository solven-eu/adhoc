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
 * Project-wide pinning tests for the interaction between JSpecify (package-level {@code @NullMarked}) and Lombok's code
 * generators. Lives in {@code adhoc-model} so any module can rely on the conclusions: the rules pinned here are "do
 * {@code @lombok.NonNull} and what you can drop" decisions that apply across the whole codebase.
 *
 * <p>
 * Bottom line for callers: until <a href="https://github.com/projectlombok/lombok/issues/3861">lombok#3861</a> is
 * fixed, package-level {@code @NullMarked} buys static-analysis nullness (NullAway, IDE highlights) but nothing at
 * runtime — Lombok's {@code @Builder} and {@code @SuperBuilder} only generate runtime null checks for fields explicitly
 * annotated with {@code @lombok.NonNull}. {@code CONVENTIONS.MD} cites this test as the empirical evidence for keeping
 * {@code @lombok.NonNull} on required builder fields.
 *
 * <p>
 * If any of these tests start failing — i.e. {@code .build()} starts throwing where it currently returns null — Lombok
 * has gained {@code @NullMarked} support at runtime. At that point reinstate the "drop {@code @NonNull}" line in
 * {@code CONVENTIONS.MD} and keep this class as the regression guard.
 *
 * <p>
 * The fixtures intentionally live in {@code eu.solven.adhoc.util} (which is {@code @NullMarked} via the production
 * {@code package-info.java} — src/main and src/test share the same package). No {@code @lombok.NonNull} on the field is
 * the load-bearing detail; everything else is boilerplate.
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
}
