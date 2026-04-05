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
package eu.solven.adhoc.security;

import lombok.experimental.UtilityClass;

/**
 * Security utilities aligned with OWASP guidance.
 *
 * <p>
 * This class provides lightweight alternatives to the full OWASP ESAPI library
 * (https://owasp.org/www-project-enterprise-security-api/) for cases where adding that dependency is not warranted.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocOWASP {

	/**
	 * Sanitizes a string for safe inclusion in log statements, preventing log-injection attacks (CWE-117, CodeQL
	 * {@code java/log-injection}).
	 *
	 * <p>
	 * Log injection works by embedding CR/LF characters in a user-controlled value to forge additional log lines.
	 * Stripping those characters is the minimal sufficient fix; see
	 * https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html#data-to-exclude and the full encoding
	 * behaviour of {@code ESAPI.encoder().encodeForHTML()} for a more complete treatment.
	 *
	 * @param value
	 *            the user-provided string to sanitize; may be {@code null}
	 * @return the sanitized string with CR, LF and other Unicode line-separator characters replaced by {@code _}, or
	 *         {@code null} if the input was {@code null}
	 */
	public static String sanitizeLog(String value) {
		if (value == null) {
			return null;
		}
		// \r\n — ASCII carriage-return / line-feed
		// \u0085 — NEXT LINE (NEL)
		// \u2028 — LINE SEPARATOR
		// \u2029 — PARAGRAPH SEPARATOR
		return value.replaceAll("[\r\n\u0085\u2028\u2029]", "_");
	}

}
