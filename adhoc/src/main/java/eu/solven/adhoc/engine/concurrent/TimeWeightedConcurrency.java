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
		return updateParallelism(1);
	}

	/**
	 * 
	 * @return the new parallelism
	 */
	public int stopConcurrentTask() {
		return updateParallelism(-1);
	}

	/**
	 * 
	 * @param delta
	 * @return the new parallelism
	 */
	public synchronized int updateParallelism(int delta) {
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

		return currentParallelism.get() + delta;
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
	public double getTimeWeightedParallelism() {
		if (time.longValue() == 0) {
			return 0D;
		} else {
			return weightedSum.doubleValue() / time.doubleValue();
		}
	}
}