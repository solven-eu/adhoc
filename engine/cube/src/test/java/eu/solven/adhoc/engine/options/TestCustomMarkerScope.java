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

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.options.CustomMarkerScope;

/**
 * Unit tests for {@link CustomMarkerScope}: pins the four observable shapes — outside-scope, scope-with-null-marker,
 * scope-with-non-null-marker, and nested scopes — that callers like {@code ICalculatedColumn} implementations rely on.
 *
 * @author Benoit Lacelle
 */
public class TestCustomMarkerScope {

	@Test
	public void testCurrent_outsideAnyScope_returnsEmpty() {
		Assertions.assertThat(CustomMarkerScope.current()).isEmpty();
		Assertions.assertThat(CustomMarkerScope.isBound()).isFalse();
	}

	@Test
	public void testRunWith_nullMarker_currentIsEmpty_isBoundIsTrue() {
		// `isBound()` exists precisely to distinguish "no scope" from "scope with null marker": for diagnostics, but
		// not as a guard — readers should treat both cases as "no marker to act on".
		CustomMarkerScope.runWith(null, () -> {
			Assertions.assertThat(CustomMarkerScope.current()).isEmpty();
			Assertions.assertThat(CustomMarkerScope.isBound()).isTrue();
			return null;
		});
		Assertions.assertThat(CustomMarkerScope.isBound()).isFalse();
	}

	@Test
	public void testRunWith_nonNullMarker_currentExposesIt() {
		String marker = "ccy=EUR";
		String returned = CustomMarkerScope.runWith(marker, () -> {
			Assertions.assertThat(CustomMarkerScope.current()).contains(marker);
			return CustomMarkerScope.current().map(Object::toString).orElse("absent");
		});
		Assertions.assertThat(returned).isEqualTo(marker);
		// Scope unbinds on exit — subsequent reads must not leak the marker.
		Assertions.assertThat(CustomMarkerScope.current()).isEmpty();
	}

	@Test
	public void testRunWith_nestedScopes_innerShadowsOuter_outerRestoredOnReturn() {
		CustomMarkerScope.runWith("outer", () -> {
			Assertions.assertThat(CustomMarkerScope.current()).contains("outer");

			CustomMarkerScope.runWith("inner", () -> {
				Assertions.assertThat(CustomMarkerScope.current()).contains("inner");
				return null;
			});

			// Inner unbound — outer is back.
			Assertions.assertThat(CustomMarkerScope.current()).contains("outer");
			return null;
		});
	}

	@Test
	public void testCallWith_propagatesCheckedExceptions() {
		Assertions.assertThatThrownBy(() -> CustomMarkerScope.callWith("m", () -> {
			throw new java.io.IOException("boom");
		})).isInstanceOf(java.io.IOException.class).hasMessageContaining("boom");
	}

	@Test
	public void testRunWith_propagatesRuntimeException() {
		Assertions.assertThatThrownBy(() -> CustomMarkerScope.runWith("m", () -> {
			throw new IllegalStateException("boom");
		})).isInstanceOf(IllegalStateException.class).hasMessageContaining("boom");
	}

	@Test
	public void testRunWith_exceptionUnbindsScope() {
		try {
			CustomMarkerScope.runWith("m", () -> {
				throw new IllegalStateException("boom");
			});
		} catch (IllegalStateException expected) {
			// Swallowed for assertion below.
		}
		// Scope MUST be unbound after the body throws — otherwise subsequent callers would leak state.
		Assertions.assertThat(CustomMarkerScope.current()).isEmpty();
		Assertions.assertThat(CustomMarkerScope.isBound()).isFalse();
	}

	@Test
	public void testCurrent_returnsOptional_notNull() {
		Optional<Object> outside = CustomMarkerScope.current();
		Assertions.assertThat(outside).isNotNull();

		CustomMarkerScope.runWith("m", () -> {
			Optional<Object> inside = CustomMarkerScope.current();
			Assertions.assertThat(inside).isNotNull();
			return null;
		});
	}
}
