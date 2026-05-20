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
package eu.solven.adhoc.engine.options;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.options.CustomMarkerScope;
import eu.solven.adhoc.engine.options.QueryOptionsScope;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.options.StandardQueryOptions;

/**
 * Unit tests for {@link QueryOptionsScope} — sibling to {@link CustomMarkerScope} with the same shape, applied to the
 * query's {@code Set<IQueryOption>} instead of a single customMarker.
 *
 * @author Benoit Lacelle
 */
public class TestQueryOptionsScope {

	@Test
	public void testCurrent_outsideAnyScope_returnsEmpty() {
		Assertions.assertThat(QueryOptionsScope.current()).isEmpty();
		Assertions.assertThat(QueryOptionsScope.isBound()).isFalse();
	}

	@Test
	public void testIsActive_outsideAnyScope_returnsFalse() {
		// No scope, no options active — readers should not have to null-check before testing.
		Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isFalse();
		Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.EXPLAIN)).isFalse();
	}

	@Test
	public void testRunWith_nullOptions_currentIsEmpty_isBoundIsTrue() {
		// null normalises to empty set — diagnostics can still distinguish via isBound().
		QueryOptionsScope.runWith(null, () -> {
			Assertions.assertThat(QueryOptionsScope.current()).isEmpty();
			Assertions.assertThat(QueryOptionsScope.isBound()).isTrue();
			return null;
		});
		Assertions.assertThat(QueryOptionsScope.isBound()).isFalse();
	}

	@Test
	public void testRunWith_nonEmptyOptions_currentExposesThem() {
		Set<IQueryOption> opts = Set.of(StandardQueryOptions.EXPLAIN, StandardQueryOptions.NO_CACHE);
		QueryOptionsScope.runWith(opts, () -> {
			Assertions.assertThat(QueryOptionsScope.current()).containsExactlyInAnyOrderElementsOf(opts);
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.EXPLAIN)).isTrue();
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isTrue();
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.DEBUG)).isFalse();
			return null;
		});
		Assertions.assertThat(QueryOptionsScope.current()).isEmpty();
	}

	@Test
	public void testRunWith_nestedScopes_innerShadowsOuter_outerRestoredOnReturn() {
		QueryOptionsScope.runWith(Set.of(StandardQueryOptions.NO_CACHE), () -> {
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isTrue();
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.EXPLAIN)).isFalse();

			QueryOptionsScope.runWith(Set.of(StandardQueryOptions.EXPLAIN), () -> {
				// Inner shadows outer entirely — NO_CACHE is no longer visible inside.
				Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.EXPLAIN)).isTrue();
				Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isFalse();
				return null;
			});

			// Outer restored on exit.
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.NO_CACHE)).isTrue();
			Assertions.assertThat(QueryOptionsScope.isActive(StandardQueryOptions.EXPLAIN)).isFalse();
			return null;
		});
	}

	@Test
	public void testRunWith_exceptionUnbindsScope() {
		try {
			QueryOptionsScope.runWith(Set.of(StandardQueryOptions.EXPLAIN), () -> {
				throw new IllegalStateException("boom");
			});
		} catch (IllegalStateException expected) {
			// Swallowed for the assertion below.
		}
		Assertions.assertThat(QueryOptionsScope.current()).isEmpty();
		Assertions.assertThat(QueryOptionsScope.isBound()).isFalse();
	}

	@Test
	public void testCurrent_isImmutable() {
		// callWith stores an ImmutableSet copy — callers cannot mutate the bound set through current().
		Set<IQueryOption> opts = Set.of(StandardQueryOptions.EXPLAIN);
		QueryOptionsScope.runWith(opts, () -> {
			Assertions.assertThatThrownBy(() -> QueryOptionsScope.current().add(StandardQueryOptions.NO_CACHE))
					.isInstanceOf(UnsupportedOperationException.class);
			return null;
		});
	}

	@Test
	public void testCallWith_propagatesCheckedExceptions() {
		Assertions.assertThatThrownBy(() -> QueryOptionsScope.callWith(Set.of(StandardQueryOptions.EXPLAIN), () -> {
			throw new java.io.IOException("boom");
		})).isInstanceOf(java.io.IOException.class).hasMessageContaining("boom");
	}
}
