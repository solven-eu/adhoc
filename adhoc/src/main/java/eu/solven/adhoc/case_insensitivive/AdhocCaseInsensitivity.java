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
package eu.solven.adhoc.case_insensitivive;

import org.jooq.conf.ParseNameCase;
import org.jooq.conf.Settings;

import lombok.Getter;
import lombok.Setter;

/**
 * Helps switching Adhoc into Case-Sensitive or Case-Insensitive.
 * 
 * @author Benoit Lacelle
 */
public class AdhocCaseInsensitivity {

	// Adhoc is currently case-sensitive b ydefault
	// But many Database (DuckDB, PostgreSQL, RedShift) are caseInsensitive
	// Some of them can be turned caseSensitive (RedShift)
	// https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html
	// Some of them will return by default the input identifiers (DuckDB)
	// https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers.html#case-sensitivity-of-identifiers
	@Setter
	@Getter
	private static boolean caseInsensitive = false;

	public static Settings jooqSettings(boolean caseInsensitive) {
		Settings settings = new Settings();

		if (!caseInsensitive) {
			// Adhoc being caseSensitive by default, we prefer to keep the case of encountered identifiers
			settings.setParseNameCase(ParseNameCase.AS_IS);
		}

		return settings;
	}

}
