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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding-window per-key rate limiter for the chat endpoint. Mechanism (3) of the scoping demonstration — caps how many
 * chat turns a single principal can fire in a given window so a runaway loop (or an abusive user) can't drain the
 * Anthropic budget.
 *
 * <p>
 * Pure in-memory (no Redis / Bucket4j dependency). Adequate for a single-node Pivotable; horizontal deployments should
 * swap in a shared store.
 *
 * <p>
 * Thread-safe via a private {@link ReentrantLock} guarding the per-key deques.
 *
 * @author Benoit Lacelle
 */
public class ChatRateLimiter {

	/** Default maximum number of chat turns allowed per principal within {@link #DEFAULT_WINDOW}. */
	public static final int DEFAULT_MAX_PER_WINDOW = 20;

	/** Default sliding window used by {@link ChatRateLimiter#ChatRateLimiter()}. */
	public static final Duration DEFAULT_WINDOW = Duration.ofMinutes(1);

	private final int maxPerWindow;

	private final Duration window;

	private final Clock clock;

	private final Map<String, Deque<Instant>> hitsByKey = new HashMap<>();

	private final ReentrantLock lock = new ReentrantLock();

	public ChatRateLimiter() {
		this(DEFAULT_MAX_PER_WINDOW, DEFAULT_WINDOW, Clock.systemUTC());
	}

	public ChatRateLimiter(int maxPerWindow, Duration window, Clock clock) {
		this.maxPerWindow = maxPerWindow;
		this.window = window;
		this.clock = clock;
	}

	/**
	 * Attempt to consume a permit for the given key (e.g. user account id, or remote IP for anonymous).
	 *
	 * @param key
	 *            principal identifier — never {@code null}; use a sentinel like {@code "anonymous"} when no user is
	 *            authenticated
	 * @return {@code true} when the request is allowed and a permit was consumed, {@code false} when the key is over
	 *         its window quota and the caller should reject with 429
	 */
	public boolean tryAcquire(String key) {
		lock.lock();
		try {
			Instant now = clock.instant();
			Instant cutoff = now.minus(window);

			Deque<Instant> hits = hitsByKey.computeIfAbsent(key, k -> new ArrayDeque<>());
			while (!hits.isEmpty() && hits.peekFirst().isBefore(cutoff)) {
				hits.pollFirst();
			}
			if (hits.size() >= maxPerWindow) {
				return false;
			}
			hits.offerLast(now);
			return true;
		} finally {
			lock.unlock();
		}
	}

	/** @return the current number of hits within the window for {@code key} — useful for diagnostic logging. */
	public int currentHits(String key) {
		lock.lock();
		try {
			Deque<Instant> hits = hitsByKey.get(key);
			if (hits == null) {
				return 0;
			}
			Instant cutoff = clock.instant().minus(window);
			while (!hits.isEmpty() && hits.peekFirst().isBefore(cutoff)) {
				hits.pollFirst();
			}
			return hits.size();
		} finally {
			lock.unlock();
		}
	}

	public int getMaxPerWindow() {
		return maxPerWindow;
	}

	public Duration getWindow() {
		return window;
	}
}
