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
package eu.solven.adhoc.pivotable.chat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestChatRateLimiter {

	private static final class MutableClock extends Clock {
		final AtomicReference<Instant> now;

		MutableClock(Instant start) {
			this.now = new AtomicReference<>(start);
		}

		void advance(Duration d) {
			now.updateAndGet(t -> t.plus(d));
		}

		@Override
		public Instant instant() {
			return now.get();
		}

		@Override
		public ZoneOffset getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(java.time.ZoneId zone) {
			throw new UnsupportedOperationException();
		}
	}

	@Test
	public void testAllowsUpToLimit_thenRejects() {
		ChatRateLimiter limiter = new ChatRateLimiter(3, Duration.ofMinutes(1), Clock.systemUTC());

		Assertions.assertThat(limiter.tryAcquire("user-a")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("user-a")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("user-a")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("user-a")).isFalse();
	}

	@Test
	public void testWindow_slidesAndReleasesCapacity() {
		MutableClock clock = new MutableClock(Instant.parse("2026-05-12T00:00:00Z"));
		ChatRateLimiter limiter = new ChatRateLimiter(2, Duration.ofMinutes(1), clock);

		Assertions.assertThat(limiter.tryAcquire("u")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("u")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("u")).isFalse();

		// Advance past the window — both prior hits expire.
		clock.advance(Duration.ofMinutes(1).plusSeconds(1));
		Assertions.assertThat(limiter.tryAcquire("u")).isTrue();
	}

	@Test
	public void testKeysAreIndependent() {
		ChatRateLimiter limiter = new ChatRateLimiter(1, Duration.ofMinutes(1), Clock.systemUTC());

		Assertions.assertThat(limiter.tryAcquire("alice")).isTrue();
		Assertions.assertThat(limiter.tryAcquire("alice")).isFalse();
		// Bob still has full quota despite Alice being capped.
		Assertions.assertThat(limiter.tryAcquire("bob")).isTrue();
	}

	@Test
	public void testCurrentHits_reportsActiveWindow() {
		MutableClock clock = new MutableClock(Instant.parse("2026-05-12T00:00:00Z"));
		ChatRateLimiter limiter = new ChatRateLimiter(5, Duration.ofMinutes(1), clock);

		limiter.tryAcquire("u");
		clock.advance(Duration.ofSeconds(30));
		limiter.tryAcquire("u");

		Assertions.assertThat(limiter.currentHits("u")).isEqualTo(2);
		Assertions.assertThat(limiter.currentHits("unknown")).isZero();
	}

	@Test
	public void testDefaults() {
		ChatRateLimiter limiter = new ChatRateLimiter();

		Assertions.assertThat(limiter.getMaxPerWindow()).isEqualTo(ChatRateLimiter.DEFAULT_MAX_PER_WINDOW);
		Assertions.assertThat(limiter.getWindow()).isEqualTo(ChatRateLimiter.DEFAULT_WINDOW);
	}
}
