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
		// Customize with `-Dadhoc.limitColumnLength=15`
		limitColumnLength = safeLoadIntegerProperty("adhoc.limitColumnLength", 1_000_000);
		// Customize with `-Dadhoc.pivotable.limitCoordinates=25000`
		limitCoordinates = safeLoadIntegerProperty("adhoc.pivotable.limitCoordinates", 100);
	}

	static int safeLoadIntegerProperty(String key, int defaultValue) {
		try {
			return Integer.getInteger(key, defaultValue);
		} catch (RuntimeException e) {
			log.warn("Issue loading -D{}={}", key, System.getProperty(key));
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
	 * Used to prevent one query consuming too much memory. This applied to both pre-aggregated columns, and
	 * transformator columns.
	 */
	public static int limitColumnLength;
}
