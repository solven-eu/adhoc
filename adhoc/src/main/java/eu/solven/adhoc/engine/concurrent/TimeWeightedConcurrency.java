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
package eu.solven.adhoc.engine.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Ticker;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * Collects statistics about a concurrent process. It evaluates
 * 
 * @author Benoit Lacelle
 * 
 */
@Builder
public class TimeWeightedConcurrency {
	private final LongAdder weightedSum = new LongAdder();
	private final LongAdder time = new LongAdder();
	private final LongAdder timeZero = new LongAdder();
	private final AtomicInteger currentParallelism = new AtomicInteger();

	@NonNull
	@Default
	private Ticker ticker = Ticker.systemTicker();

	@Default
	private AtomicLong lastTimestamp = new AtomicLong();

	/**
	 * 
	 * @return the new parallelism
	 */
	public int startConcurrentTask() {
		return addParallelism(1);
	}

	/**
	 * 
	 * @return the new parallelism
	 */
	public int stopConcurrentTask() {
		return addParallelism(-1);
	}

	/**
	 * 
	 * @param delta
	 * @return the new parallelism
	 */
	@SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel")
	public synchronized int addParallelism(int delta) {
		int parallelism = currentParallelism.getAndAdd(delta);

		long now = ticker.read();
		long durationWithParallelism = now - lastTimestamp.get();
		if (parallelism > 0) {
			weightedSum.add(parallelism * durationWithParallelism);
			time.add(durationWithParallelism);
		} else if (parallelism == 0) {
			timeZero.add(durationWithParallelism);
		}
		lastTimestamp.set(now);

		return parallelism + delta;
	}

	/**
	 * 
	 * @return the number of currently active tasks.
	 */
	public int getCurrentParallelism() {
		return currentParallelism.get();
	}

	/**
	 * 
	 * @return the mean parallelism, based on period of time with at least one active task.
	 */
	@SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel")
	public synchronized double getTimeWeightedParallelism() {
		if (time.longValue() == 0) {
			return 0D;
		} else {
			return weightedSum.doubleValue() / time.doubleValue();
		}
	}
}