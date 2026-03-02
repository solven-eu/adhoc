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
package eu.solven.adhoc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

import cz.jirutka.rsql.parser.RSQLParserException;
import eu.solven.adhoc.filter.rsql.RsqlToAdhocFilter;

/**
 * Coverage-guided fuzz test for {@link RsqlToAdhocFilter}.
 *
 * <p>
 * Jazzer feeds mutated byte sequences as arbitrary {@link String} input to the RSQL parser. The goal is to detect
 * crashes, infinite loops, or unexpected exceptions that would indicate a bug in the parser or the filter-construction
 * logic.
 *
 * <p>
 * When run normally (e.g. {@code mvn test}), Jazzer executes a short fixed set of seed inputs to verify the harness
 * compiles and the parser handles known-good and known-bad strings. For continuous fuzzing, run with
 * {@code -Djazzer.fuzz} or integrate with OSS-Fuzz / ClusterFuzz.
 *
 * @see <a href="https://github.com/CodeIntelligenceTesting/jazzer">Jazzer</a>
 * @see <a href="https://github.com/CodeIntelligenceTesting/jazzer/blob/main/docs/junit-integration.md">JUnit 5
 *      integration</a>
 */
public class FuzzRsqlToAdhocFilter {

	// Shared instance: parser is stateless, so sharing is safe and avoids per-call allocation.
	private static final RsqlToAdhocFilter CONVERTER = new RsqlToAdhocFilter();

	/**
	 * Fuzz entry point called by Jazzer with mutated input.
	 *
	 * <p>
	 * Only {@link RSQLParserException} and {@link IllegalArgumentException} are treated as expected rejection signals
	 * for malformed input. Any other {@link Throwable} propagates and is reported by Jazzer as a finding.
	 *
	 * @param data
	 *            Jazzer-controlled byte source; consumed as a UTF-8 string.
	 */
	@FuzzTest
	public void fuzz(FuzzedDataProvider data) {
		String rsql = data.consumeRemainingAsString();
		try {
			CONVERTER.rsql(rsql);
		} catch (RSQLParserException e) {
			// Expected: malformed RSQL syntax is rejected by the parser.
		} catch (IllegalArgumentException e) {
			// Expected: certain degenerate inputs (e.g. empty string) may raise this.
		}
	}
}
