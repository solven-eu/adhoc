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
package eu.solven.adhoc.pivotable.util;

import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds sensitive configuration for Pivotable module.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
@SuppressWarnings("PMD.FieldDeclarationsShouldBeAtStartOfClass")
public class PivotableUnsafe {

	public static void resetProperties() {
		log.info("Resetting PivotableUnsafe configuration");

		limitCoordinates = DEFAULT_LIMIT_COORDINATES;
	}

	public static void resetAll() {
		resetProperties();
	}

	public static void reloadProperties() {
		// Customize with `-Dadhoc.pivotable.limitCoordinates=25000`
		limitCoordinates =
				AdhocUnsafe.safeLoadIntegerProperty("adhoc.pivotable.limitCoordinates", DEFAULT_LIMIT_COORDINATES);
	}

	private static final int DEFAULT_LIMIT_COORDINATES = 100;
	/**
	 * Used as default number of examples coordinates when fetching columns by API.
	 */
	// TODO This should be a pivotable custom parameter
	@Setter
	@Getter
	private static int limitCoordinates = DEFAULT_LIMIT_COORDINATES;
}
