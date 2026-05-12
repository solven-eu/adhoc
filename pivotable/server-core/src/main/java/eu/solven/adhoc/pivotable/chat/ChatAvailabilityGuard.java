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
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

/**
 * Tracks whether the AI chat assistant is currently usable. When the upstream provider returns a long-term failure
 * (e.g. the Anthropic account is out of credit, or the API key was revoked), callers mark the guard
 * {@linkplain #markUnavailableFor(Duration, String) unavailable for a cooldown window}, after which
 * {@link #isAvailable()} flips back to {@code true} automatically.
 *
 * <p>
 * The probe endpoint ({@code GET /api/v1/cubes/chat/enabled}) consults this guard, so the SPA's floating chatbot icon
 * self-hides on the next page load when the guard is tripped.
 *
 * <p>
 * Thread-safe — backed by an {@link AtomicReference}.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class ChatAvailabilityGuard {

	/** Substring used to detect Anthropic's "out of credit" error in a response body — case-insensitive match. */
	public static final String CREDIT_ERROR_MARKER = "credit balance";

	/** Default cooldown applied when an Anthropic billing/credit error is observed. */
	public static final Duration DEFAULT_COOLDOWN = Duration.ofHours(1);

	private final Clock clock;

	private final AtomicReference<Instant> unavailableUntil = new AtomicReference<>();

	public ChatAvailabilityGuard() {
		this(Clock.systemUTC());
	}

	public ChatAvailabilityGuard(Clock clock) {
		this.clock = clock;
	}

	/**
	 * @return {@code true} when callers should treat the chat as usable. Returns {@code false} during an active
	 *         cooldown window set by {@link #markUnavailableFor(Duration, String)}.
	 */
	public boolean isAvailable() {
		return disabledUntil().isEmpty();
	}

	/**
	 * @return the {@link Instant} until which the guard is tripped, or {@link Optional#empty()} when the chat is
	 *         available now. The cooldown self-clears once {@code now &gt;= until}.
	 */
	public Optional<Instant> disabledUntil() {
		Instant until = unavailableUntil.get();
		if (until == null || !clock.instant().isBefore(until)) {
			return Optional.empty();
		}
		return Optional.of(until);
	}

	/**
	 * Mark the chat unavailable for the given duration. A second call within an existing cooldown does NOT extend the
	 * window unless the new deadline is later than the existing one — this keeps a stream of identical failures from
	 * ratcheting the cooldown forward indefinitely.
	 *
	 * @param cooldown
	 *            how long to keep the chat disabled, measured from now
	 * @param reason
	 *            a short, log-friendly explanation (printed at WARN level)
	 */
	public void markUnavailableFor(Duration cooldown, String reason) {
		Instant newDeadline = clock.instant().plus(cooldown);
		Instant previous = unavailableUntil.getAndAccumulate(newDeadline, (existing, candidate) -> {
			if (existing == null || existing.isBefore(candidate)) {
				return candidate;
			}
			return existing;
		});
		if (previous == null || previous.isBefore(newDeadline)) {
			log.warn("Pivotable chat disabled until {} — {}", newDeadline, reason);
		}
	}

	/**
	 * Inspect an upstream-provider error body and trip the guard with {@link #DEFAULT_COOLDOWN} when it matches a known
	 * long-term failure pattern. Currently triggers on Anthropic's "credit balance too low" response.
	 *
	 * @param responseBody
	 *            the body returned by Anthropic alongside a non-2xx status
	 * @return {@code true} if the guard was tripped as a result of this call
	 */
	public boolean tripIfLongTermFailure(String responseBody) {
		if (responseBody != null && responseBody.toLowerCase(Locale.ROOT).contains(CREDIT_ERROR_MARKER)) {
			markUnavailableFor(DEFAULT_COOLDOWN,
					"Anthropic reported credit-balance-too-low — cooldown to avoid hammering the API");
			return true;
		}
		return false;
	}
}
