package eu.solven.adhoc.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enable naming of ForkJoinPool threads
 *
 * @author Benoit Lacelle
 *
 * @see <a
 *      href="http://stackoverflow.com/questions/34303094/is-it-not-possible-to-supply-a-thread-facory-or-name-pattern-to-forkjoinpool>StackOverFlow</a>
 */
public class NamingForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

	protected final String threadPrefix;

	// For some reason `worker.getPoolIndex()` always returns `0`
	protected final AtomicLong nextThreadIndex = new AtomicLong();

	public NamingForkJoinWorkerThreadFactory(String threadPrefix) {
		this.threadPrefix = threadPrefix;
	}

	@Override
	public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
		worker.setName(threadPrefix + nextThreadIndex.getAndIncrement());
		return worker;
	}
}
