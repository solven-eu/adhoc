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

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

/**
 * Some various unsafe constants, one should edit if he knows what he's doing.
 */
@Slf4j
public class AdhocUnsafe {

	static {
		reloadProperties();
	}

	public static void reloadProperties() {
		// Customize with `-Dadhoc.limitOrdinalToString=15`
		limitOrdinalToString = safeLoadIntegerProperty("adhoc.limitOrdinalToString", 5);
		// Customize with `-Dadhoc.limitColumnSize=15`
		limitColumnSize = safeLoadIntegerProperty("adhoc.limitColumnSize", 1_000_000);
		// Customize with `-Dadhoc.pivotable.limitCoordinates=25000`
		limitCoordinates = safeLoadIntegerProperty("adhoc.pivotable.limitCoordinates", 100);
		// Customize with `-Dadhoc.failfast=false`
		failFast = safeLoadBooleanProperty("adhoc.failfast", true);
		// Customize with `-Dadhoc.parallelism=16`
		parallelism = safeLoadIntegerProperty("adhoc.parallelism", defaultParallelism());
		// Customize with `-Dadhoc.defaultColumnCapacity=100_000`
		defaultColumnCapacity = safeLoadIntegerProperty("adhoc.defaultColumnCapacity", limitColumnSize);
	}

	static int safeLoadIntegerProperty(String key, int defaultValue) {
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
	public static int limitOrdinalToString;

	/**
	 * Used as default number of examples coordinates when fetching columns by API.
	 */
	// TODO This should be a pivotable custom parameter
	public static int limitCoordinates;

	/**
	 * This will limit data-structures to go over this size.
	 * 
	 * Used to prevent one query consuming too much memory. This applied to both pre-aggregated columns, and
	 * transformator columns.
	 */
	public static int limitColumnSize;

	/**
	 * On multiple occasions, we encounter exceptions which are not fatal. Should we be resilient, or fail-fast?
	 */
	// By default, failFast. This is a simple flag for projects preferring resiliency.
	public static boolean failFast;

	private static int parallelism;

	private static int defaultColumnCapacity;

	/**
	 * Used for unitTests
	 */
	public static boolean deterministicUUID = false;
	public static AtomicLong deterministicUUIDIndex = new AtomicLong();
	public static AtomicLong deterministicQueryIndex = new AtomicLong();

	public static UUID randomUUID() {
		if (deterministicUUID) {
			String uuidIndexAsString = Long.toString(deterministicUUIDIndex.getAndIncrement());
			return UUID.fromString("00000000-0000-0000-0000-" + Strings.padStart(uuidIndexAsString, 12, '0'));
		} else {
			return UUID.randomUUID();
		}
	}

	public static void resetDeterministicQueryIds() {
		if (!AdhocUnsafe.deterministicUUID) {
			log.warn("queryIds are now deterministic. This should not happen in PRD");
		}
		AdhocUnsafe.deterministicUUID = true;
		AdhocUnsafe.deterministicUUIDIndex.set(0);
		AdhocUnsafe.deterministicQueryIndex.set(0);
	}

	public static long nextQueryIndex() {
		return deterministicQueryIndex.getAndIncrement();
	}

	private static int defaultParallelism() {
		// Multiply by 2 as we expect some thread to be stuck on external calls like a `ITableQuery` waiting for the
		// computation by an external ITableWrapper
		return Runtime.getRuntime().availableProcessors() * 2;
	}

	/**
	 * @return the default parallelism when calling Executors#newWorkStealingPool
	 */
	public static int parallelism() {
		return parallelism;
	}

	// https://stackoverflow.com/questions/47261001/is-it-beneficial-to-use-forkjoinpool-as-usual-executorservice
	public static ExecutorService adhocCommonPool = Executors.newWorkStealingPool(AdhocUnsafe.parallelism());

	/**
	 * Relates with {@value #limitColumnSize}, as if a data-structure has the same capacity and maximum size, it is
	 * guaranteed never to need being grown/re-hashed.
	 * 
	 * @return the default capacity for structured.
	 */
	public static int defaultCapacity() {
		if (defaultColumnCapacity >= 0) {
			// the default capacity is capped by the limit over columnLength
			return Math.min(limitColumnSize, defaultColumnCapacity);
		}

		// There is no default capacity: fallback on limitColumnSize. This way, data-structures would never grow nor
		// re-hash.
		return limitColumnSize;
	}
}
