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

public class TestChatAvailabilityGuard {

	// A clock whose "now" is mutable, so tests can advance time without sleeping.
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
	public void testFreshGuard_isAvailable() {
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard();

		Assertions.assertThat(guard.isAvailable()).isTrue();
		Assertions.assertThat(guard.disabledUntil()).isEmpty();
	}

	@Test
	public void testMarkUnavailable_thenAutoExpires() {
		MutableClock clock = new MutableClock(Instant.parse("2026-05-12T00:00:00Z"));
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard(clock);

		guard.markUnavailableFor(Duration.ofHours(1), "test");
		Assertions.assertThat(guard.isAvailable()).isFalse();
		Assertions.assertThat(guard.disabledUntil()).contains(Instant.parse("2026-05-12T01:00:00Z"));

		// 59 minutes later: still tripped.
		clock.advance(Duration.ofMinutes(59));
		Assertions.assertThat(guard.isAvailable()).isFalse();

		// At exactly 1 hour: cooldown over (boundary is inclusive of the past).
		clock.advance(Duration.ofMinutes(1));
		Assertions.assertThat(guard.isAvailable()).isTrue();
		Assertions.assertThat(guard.disabledUntil()).isEmpty();
	}

	@Test
	public void testMarkUnavailable_doesNotShortenExistingCooldown() {
		MutableClock clock = new MutableClock(Instant.parse("2026-05-12T00:00:00Z"));
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard(clock);

		guard.markUnavailableFor(Duration.ofHours(1), "first");
		Instant firstDeadline = guard.disabledUntil().orElseThrow();

		// A subsequent shorter cooldown call must NOT pull the deadline forward.
		guard.markUnavailableFor(Duration.ofMinutes(5), "shorter");
		Assertions.assertThat(guard.disabledUntil()).contains(firstDeadline);
	}

	@Test
	public void testMarkUnavailable_extendsCooldownWhenLonger() {
		MutableClock clock = new MutableClock(Instant.parse("2026-05-12T00:00:00Z"));
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard(clock);

		guard.markUnavailableFor(Duration.ofMinutes(10), "short");
		guard.markUnavailableFor(Duration.ofHours(2), "longer");

		Assertions.assertThat(guard.disabledUntil()).contains(Instant.parse("2026-05-12T02:00:00Z"));
	}

	@Test
	public void testTripIfLongTermFailure_creditBalance_trips() {
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard();

		boolean tripped = guard.tripIfLongTermFailure(
				"{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API.\"}}");

		Assertions.assertThat(tripped).isTrue();
		Assertions.assertThat(guard.isAvailable()).isFalse();
	}

	@Test
	public void testTripIfLongTermFailure_unrelatedError_doesNotTrip() {
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard();

		boolean tripped = guard.tripIfLongTermFailure(
				"{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}");

		Assertions.assertThat(tripped).isFalse();
		Assertions.assertThat(guard.isAvailable()).isTrue();
	}

	@Test
	public void testTripIfLongTermFailure_nullBody_doesNotTrip() {
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard();

		Assertions.assertThat(guard.tripIfLongTermFailure(null)).isFalse();
		Assertions.assertThat(guard.isAvailable()).isTrue();
	}

	@Test
	public void testTripIfLongTermFailure_caseInsensitive() {
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard();

		Assertions.assertThat(guard.tripIfLongTermFailure("Credit Balance too low")).isTrue();
	}
}
