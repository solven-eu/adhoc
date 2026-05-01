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
package eu.solven.adhoc.exception;

import java.util.concurrent.CompletionException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocExceptionHelpers {

	@Test
	public void testWrap_illegalState_keepsIllegalState() {
		// `IllegalStateException` flagged a developer-facing invariant break — wrap() must preserve that intent
		// (otherwise it would be downgraded to an `IllegalArgumentException` and treated as caller fault).
		IllegalStateException cause = new IllegalStateException("invariant broken");

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", cause);

		Assertions.assertThat(wrapped).isInstanceOf(IllegalStateException.class).hasMessage("ctx").hasCause(cause);
	}

	@Test
	public void testWrap_completionWrappingIllegalState_keepsIllegalState() {
		// CompletableFuture's typical wrapping: CompletionException(IllegalStateException). The helper unwraps
		// the cause kind to keep IllegalStateException semantics, even though the immediate wrapped is the
		// CompletionException itself.
		IllegalStateException cause = new IllegalStateException("inner");
		CompletionException completion = new CompletionException(cause);

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", completion);

		Assertions.assertThat(wrapped).isInstanceOf(IllegalStateException.class).hasMessage("ctx").hasCause(completion);
	}

	@Test
	public void testWrap_completionWrappingOther_becomesIllegalArgument() {
		// CompletionException with a non-IllegalStateException cause → IllegalArgumentException ("caller fault").
		RuntimeException inner = new RuntimeException("inner");
		CompletionException completion = new CompletionException(inner);

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", completion);

		Assertions.assertThat(wrapped)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("ctx")
				.hasCause(completion);
	}

	@Test
	public void testWrap_completionWithNullCause_becomesIllegalArgument() {
		// CompletionException with no cause: instanceof IllegalStateException check on null returns false → falls
		// through to the IllegalArgumentException branch.
		CompletionException completion = new CompletionException(null);

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", completion);

		Assertions.assertThat(wrapped)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("ctx")
				.hasCause(completion);
	}

	@Test
	public void testWrap_arbitraryRuntime_becomesIllegalArgument() {
		RuntimeException original = new RuntimeException("boom");

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", original);

		Assertions.assertThat(wrapped)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("ctx")
				.hasCause(original);
	}

	@Test
	public void testWrap_illegalArgument_becomesIllegalArgument() {
		// IllegalArgumentException is not an IllegalStateException → treated as a generic RuntimeException and
		// re-wrapped as IllegalArgumentException with the new message.
		IllegalArgumentException original = new IllegalArgumentException("bad arg");

		RuntimeException wrapped = AdhocExceptionHelpers.wrap("ctx", original);

		Assertions.assertThat(wrapped)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("ctx")
				.hasCause(original);
	}
}
