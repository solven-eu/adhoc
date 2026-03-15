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
package eu.solven.adhoc.encoding.column;

import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some various unsafe constants related to columns, one should edit if he knows what he's doing.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.FieldDeclarationsShouldBeAtStartOfClass" })
public class AdhocColumnUnsafe {
	static {
		reloadProperties();
	}

	public static void resetProperties() {
		log.info("Resetting {} configuration", AdhocColumnUnsafe.class.getName());

		limitColumnSize = 1_000_000;
		defaultColumnCapacity = limitColumnSize;
		pageSize = DEFAULT_PAGE_SIZE;
	}

	public static void resetAll() {
		resetProperties();

	}

	public static void reloadProperties() {
		// Customize with `-Dadhoc.limitColumnSize=10000`
		limitColumnSize = AdhocUnsafe.safeLoadIntegerProperty("adhoc.limitColumnSize", 1_000_000);
		// Customize with `-Dadhoc.defaultColumnCapacity=100_000`
		defaultColumnCapacity = AdhocUnsafe.safeLoadIntegerProperty("adhoc.defaultColumnCapacity", limitColumnSize);
		pageSize = AdhocUnsafe.safeLoadIntegerProperty("adhoc.pageSize", DEFAULT_PAGE_SIZE);
	}

	/**
	 * This will limit data-structures to go over this size.
	 * 
	 * Used to prevent one query consuming too much memory. This applied to both pre-aggregated columns, and
	 * transformator columns.
	 */
	@Setter
	@Getter
	private static int limitColumnSize;

	/**
	 * Used as default capacity when allocating chunks of data.
	 */
	@Setter
	private static int defaultColumnCapacity;

	private static final int DEFAULT_PAGE_SIZE = 16 * 1024;
	@Setter
	@Getter
	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Relates with {@value #limitColumnSize}, as if a data-structure has the same capacity and maximum size, it is
	 * guaranteed never to need being grown/re-hashed.
	 * 
	 * @return the default capacity for structured.
	 */
	public static int getDefaultColumnCapacity() {
		if (defaultColumnCapacity >= 0) {
			// the default capacity is capped by the limit over columnLength
			return Math.min(limitColumnSize, defaultColumnCapacity);
		}

		// There is no default capacity: fallback on limitColumnSize. This way, data-structures would never grow nor
		// re-hash.
		return limitColumnSize;
	}

	public static void checkColumnSize(long size) {
		if (size >= getLimitColumnSize()) {
			throw new IllegalStateException(
					"Can not add as size=%s and limit=%s Consider `AdhocUnsafe.setLimitColumnSize(X)` or -Dadhoc.limitColumnSize=X"
							.formatted(size, AdhocColumnUnsafe.getLimitColumnSize()));
		}
	}
}
