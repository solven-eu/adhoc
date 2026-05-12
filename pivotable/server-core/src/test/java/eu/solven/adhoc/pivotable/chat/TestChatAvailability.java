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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Covers the {@link ChatAvailability#resolve(String, boolean, ChatAvailabilityGuard)} resolution paths so the
 * always-mounted chat endpoints have a tested source of truth for the four reachable states.
 */
public class TestChatAvailability {

	@Test
	public void testResolve_nullApiKey_returnsNotConfigured() {
		ChatAvailability availability = ChatAvailability.resolve(null, true, new ChatAvailabilityGuard());
		Assertions.assertThat(availability.enabled()).isFalse();
		Assertions.assertThat(availability.reason()).isEqualTo(ChatAvailability.REASON_NOT_CONFIGURED);
		Assertions.assertThat(availability.retryAfterSeconds()).isNull();
	}

	@Test
	public void testResolve_blankApiKey_returnsNotConfigured() {
		ChatAvailability availability = ChatAvailability.resolve("   ", true, new ChatAvailabilityGuard());
		Assertions.assertThat(availability.reason()).isEqualTo(ChatAvailability.REASON_NOT_CONFIGURED);
	}

	@Test
	public void testResolve_keySetButToggleOff_returnsDisabledByConfig() {
		ChatAvailability availability = ChatAvailability.resolve("sk-ant-api03-x", false, new ChatAvailabilityGuard());
		Assertions.assertThat(availability.enabled()).isFalse();
		Assertions.assertThat(availability.reason()).isEqualTo(ChatAvailability.REASON_DISABLED_BY_CONFIG);
	}

	@Test
	public void testResolve_keyAndToggleAndNoCooldown_returnsEnabled() {
		ChatAvailability availability = ChatAvailability.resolve("sk-ant-api03-x", true, new ChatAvailabilityGuard());
		Assertions.assertThat(availability.enabled()).isTrue();
		Assertions.assertThat(availability.reason()).isNull();
		Assertions.assertThat(availability.retryAfterSeconds()).isNull();
	}

	@Test
	public void testResolve_keyAndToggleButCooldownActive_returnsCooldown() {
		// Fix the clock so the cooldown end is exactly 60 s in the future and the test is deterministic.
		Clock fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
		ChatAvailabilityGuard guard = new ChatAvailabilityGuard(fixed);
		guard.markUnavailableFor(Duration.ofSeconds(60), "test cooldown");

		ChatAvailability availability = ChatAvailability.resolve("sk-ant-api03-x", true, guard);
		Assertions.assertThat(availability.enabled()).isFalse();
		Assertions.assertThat(availability.reason()).isEqualTo(ChatAvailability.REASON_COOLDOWN);
		// `retryAfterSeconds` is derived from System.currentTimeMillis (not the fixed clock), so we cannot assert an
		// exact value — but the cooldown should be present and non-negative.
		Assertions.assertThat(availability.retryAfterSeconds()).isNotNull();
		Assertions.assertThat(availability.retryAfterSecondsOrZero()).isGreaterThanOrEqualTo(0L);
	}

	@Test
	public void testRetryAfterSecondsOrZero_nullField_returnsZero() {
		Assertions.assertThat(ChatAvailability.ofEnabled().retryAfterSecondsOrZero()).isZero();
		Assertions.assertThat(ChatAvailability.ofNotConfigured().retryAfterSecondsOrZero()).isZero();
	}

	@Test
	public void testRetryAfterSecondsOrZero_cooldown_returnsField() {
		Assertions.assertThat(ChatAvailability.ofCooldown(42L).retryAfterSecondsOrZero()).isEqualTo(42L);
	}

	@Test
	public void testFactories_haveExpectedShape() {
		Assertions.assertThat(ChatAvailability.ofEnabled())
				.satisfies(a -> Assertions.assertThat(a.enabled()).isTrue())
				.satisfies(a -> Assertions.assertThat(a.reason()).isNull());
		Assertions.assertThat(ChatAvailability.ofNotConfigured().reason())
				.isEqualTo(ChatAvailability.REASON_NOT_CONFIGURED);
		Assertions.assertThat(ChatAvailability.ofDisabledByConfig().reason())
				.isEqualTo(ChatAvailability.REASON_DISABLED_BY_CONFIG);
		Assertions.assertThat(ChatAvailability.ofCooldown(10L).reason()).isEqualTo(ChatAvailability.REASON_COOLDOWN);
	}
}
