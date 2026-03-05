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
package eu.solven.adhoc.util;

import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import eu.solven.adhoc.collection.ComparableElseClassComparator;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some various unsafe constants, one should edit if he knows what he's doing.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.MutableStaticState", "PMD.FieldDeclarationsShouldBeAtStartOfClass" })
public class AdhocUnsafe {
	static {
		reloadProperties();
	}

	public static void resetProperties() {
		log.info("Resetting {} configuration", AdhocUnsafe.class.getName());

		// This default should be big enough to show all informations in most cases, without printing huge information
		// on edge-cases
		// Typically, 5 is too small as many projects generates more than 5 filtered columns
		limitOrdinalToString = 16;
		failFast = true;
		parallelism = defaultParallelism();
		cartesianProductLimit = DEFAULT_CARTESIAN_PRODUCT_LIMIT;
		setNullComparator(DEFAULT_NULL_COMPARATOR);
	}

	public static void resetAll() {
		resetProperties();
	}

	public static void reloadProperties() {
		// Customize with `-Dadhoc.limitOrdinalToString=32`
		limitOrdinalToString = safeLoadIntegerProperty("adhoc.limitOrdinalToString", 16);
		// Customize with `-Dadhoc.pivotable.limitCoordinates=25000`
		// Customize with `-Dadhoc.failfast=false`
		failFast = safeLoadBooleanProperty("adhoc.failfast", true);
		// Customize with `-Dadhoc.parallelism=16`
		parallelism = safeLoadIntegerProperty("adhoc.parallelism", defaultParallelism());
		cartesianProductLimit = safeLoadIntegerProperty("adhoc.cartesianProductLimit", DEFAULT_CARTESIAN_PRODUCT_LIMIT);
	}

	public static int safeLoadIntegerProperty(String key, int defaultValue) {
		try {
			return Integer.getInteger(key, defaultValue);
		} catch (RuntimeException e) {
			log.warn("Issue loading -D{}={}", key, System.getProperty(key));
		}
		return defaultValue;
	}

	static boolean safeLoadBooleanProperty(String key, boolean defaultValue) {
		// Duplicate `Boolean.getBoolean` but enabling a `true` defaultValue
		String property = System.getProperty(key);
		try {
			boolean result = defaultValue;
			try {
				if (property != null) {
					result = Boolean.parseBoolean(property);
				}
			} catch (IllegalArgumentException | NullPointerException e) {
				log.trace("Issue parsing boolean for key={}", key, e);
			}
			return result;
		} catch (RuntimeException e) {
			log.warn("Issue loading -D{}={}", key, property);
		}
		return defaultValue;
	}

	/**
	 * In various `.toString`, we print only a given number of elements, to prevent the {@link String} to grow too big.
	 */
	@Setter
	@Getter
	private static int limitOrdinalToString;

	/**
	 * On multiple occasions, we encounter exceptions which are not fatal. Should we be resilient, or fail-fast?
	 */
	// By default, failFast. This is a simple flag for projects preferring resiliency.
	@Getter
	private static boolean failFast;

	private static int parallelism;

	/**
	 * Used for unitTests
	 */
	public static boolean deterministicUUID = false;
	private static final AtomicLong DETERMINISTIC_UUID_INDEX = new AtomicLong();
	private static final AtomicLong DETERMINISTIC_QUERY_INDEX = new AtomicLong();
	private static final AtomicLong DETERMINISTIC_QUERY_STEP_INDEX = new AtomicLong();

	public static UUID randomUUID() {
		if (deterministicUUID) {
			String uuidIndexAsString = Long.toString(DETERMINISTIC_UUID_INDEX.getAndIncrement());
			return UUID.fromString("00000000-0000-0000-0000-" + Strings.padStart(uuidIndexAsString, 12, '0'));
		} else {
			return UUID.randomUUID();
		}
	}

	public static void resetDeterministicQueryIds() {
		if (!deterministicUUID) {
			log.warn("queryIds are now deterministic. This should not happen in PRD");
		}
		deterministicUUID = true;
		DETERMINISTIC_UUID_INDEX.set(0);
		DETERMINISTIC_QUERY_INDEX.set(0);
		DETERMINISTIC_QUERY_STEP_INDEX.set(0);
	}

	/**
	 * 
	 * @return a long identifying in current ClassLoader the next query
	 */
	public static long nextQueryIndex() {
		return DETERMINISTIC_QUERY_INDEX.getAndIncrement();
	}

	/**
	 * 
	 * @return a long identifying in current ClassLoader the next query step
	 */
	public static long nextQueryStepIndex() {
		return DETERMINISTIC_QUERY_STEP_INDEX.getAndIncrement();
	}

	private static int defaultParallelism() {
		// Multiply by 2 as we expect some thread to be stuck on external calls like a `ITableQuery` waiting for the
		// computation by an external ITableWrapper
		return Runtime.getRuntime().availableProcessors() * 2;
	}

	/**
	 * @return the default parallelism when calling Executors#newWorkStealingPool
	 */
	public static int getParallelism() {
		return parallelism;
	}

	// https://stackoverflow.com/questions/47261001/is-it-beneficial-to-use-forkjoinpool-as-usual-executorservice
	public static ListeningExecutorService adhocCommonPool = MoreExecutors.listeningDecorator(newWorkStealingPool());

	/**
	 * Similar with java.util.concurrent.Executors.newWorkStealingPool(int), but with a custom name.
	 * 
	 * @return
	 */
	private static ForkJoinPool newWorkStealingPool() {
		return new ForkJoinPool(getParallelism(), new NamingForkJoinWorkerThreadFactory("adhoc-common-"), null, true);
	}

	// Typically used as limit to prevent iterating over large cartesian products
	// This limit should be applied over the number of potential combinations
	public static int cartesianProductLimit = 16 * 1024;
	private static final int DEFAULT_CARTESIAN_PRODUCT_LIMIT = 16 * 1024;

	// A pool dedicated to maintenance operations.
	// Typically used in `CacheBuilder.refreshAfterWrite(_)` scenarios
	public static Executor maintenancePool = Executors.newCachedThreadPool(Thread.ofPlatform()
			// Daemon as these thread does not hold critical operations
			.daemon(true)
			.name("adhoc-maintenance-", 0)
			.factory());

	public static void inFailFast() {
		if (!failFast) {
			log.info("Switching failfast=true");
			failFast = true;
		}
	}

	public static void outFailFast() {
		if (failFast) {
			log.info("Switching failfast=false");
			failFast = false;
		}
	}

	public static final Comparator<Object> DEFAULT_NULL_COMPARATOR = ComparableElseClassComparator.nullsHigh();
	@Getter
	private static Comparator<Object> nullComparator = DEFAULT_NULL_COMPARATOR;

	public static void setNullComparator(Comparator<Object> nullComparator) {
		AdhocUnsafe.nullComparator = nullComparator;
		valueComparator = new ComparableElseClassComparator(AdhocUnsafe.nullComparator);
	}

	@Getter
	private static Comparator<Object> valueComparator = new ComparableElseClassComparator(nullComparator);
}
