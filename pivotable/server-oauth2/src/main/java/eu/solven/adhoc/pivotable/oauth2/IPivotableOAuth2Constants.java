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
package eu.solven.adhoc.pivotable.oauth2;

/**
 * Constants related with OAuth2 configuration in Pivotable.
 * 
 * @author Benoit Lacelle
 */
public interface IPivotableOAuth2Constants {
	// https://connect2id.com/products/server/docs/api/token#url
	String KEY_OAUTH2_ISSUER = "adhoc.pivotable.oauth2.issuer-base-url";
	String KEY_JWT_SIGNINGKEY = "adhoc.pivotable.oauth2.signing-key";

	/**
	 * Validity of an issued {@code access_token}, as an ISO-8601 duration string (e.g. {@code "PT1H"}). Default is one
	 * hour — short enough that a leaked token has a small blast radius, long enough that refreshes are rare in normal
	 * UX. Override to a few seconds under a test profile (e.g. {@code pivotable-e2e-shorttoken}) to exercise the
	 * refresh path end-to-end without waiting the real hour.
	 */
	String KEY_ACCESS_TOKEN_VALIDITY = "adhoc.pivotable.oauth2.access-token.validity";
	String DEFAULT_ACCESS_TOKEN_VALIDITY = "PT1H";

	/**
	 * Validity of an issued {@code refresh_token}, as an ISO-8601 duration string (e.g. {@code "P365D"}). Default is
	 * 365 days (~remember-me semantics: stay logged in for a year). Override to a few seconds under a test profile to
	 * exercise the "must re-login" path without running the test for days.
	 */
	String KEY_REFRESH_TOKEN_VALIDITY = "adhoc.pivotable.oauth2.refresh-token.validity";
	String DEFAULT_REFRESH_TOKEN_VALIDITY = "P365D";

	/**
	 * Used to generate a signingKey on the fly. Useful for integrationTests
	 */
	String GENERATE = "GENERATE";
}
