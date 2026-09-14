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
package eu.solven.adhoc.pivotable.chat;

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;

/**
 * Server-side filter applied to each {@code text} chunk streamed back from the model. Mechanism (4) of the scoping
 * demonstration — a defence-in-depth layer that redacts known-dangerous substrings (credentials, AWS keys, plausible
 * shell commands) before they reach the SPA.
 *
 * <p>
 * Mostly theatre against a competent attacker — the patterns here are easy to evade. The point is to make accidental
 * leakage visible and to give the chat assistant a clear "this would be redacted" boundary. A real deployment would
 * extend the {@link #PATTERNS} list with project-specific secrets and review false-positive rates.
 *
 * @author Benoit Lacelle
 */
public class ChatOutputFilter {

	/** Replacement token surfaced in place of any matched pattern. */
	public static final String REDACTED = "[REDACTED]";

	/**
	 * Pattern catalogue, kept intentionally small. Add domain-specific patterns (internal hostnames, project secrets,
	 * regex of customer PII formats) as the deployment requires.
	 */
	static final List<Pattern> PATTERNS = ImmutableList.of(
			// AWS access-key IDs (AKIA / ASIA prefix, 20 char base32)
			Pattern.compile("\\bA[KS]IA[0-9A-Z]{16}\\b"),
			// AWS secret keys (40 char base64-ish)
			Pattern.compile("\\b(?<![A-Za-z0-9])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9])"),
			// 16-digit numbers that look like a payment card (Luhn-agnostic — false positives possible)
			Pattern.compile("\\b\\d{4}[ -]?\\d{4}[ -]?\\d{4}[ -]?\\d{4}\\b"),
			// Anthropic-style API keys / OAuth tokens
			Pattern.compile("\\bsk-ant-(?:api|oat)\\w{2}-[A-Za-z0-9_-]{20,}\\b"),
			// `rm -rf` style shell hazards (the model has no shell access, but redacting these prevents social-
			// engineering attempts to get the user to copy-paste a malicious snippet).
			Pattern.compile("(?i)\\brm\\s+-rf\\s+/"));

	/**
	 * @param chunk
	 *            text chunk emitted by the model — never {@code null}
	 * @return the same chunk with any matched pattern replaced by {@link #REDACTED}
	 */
	public String filter(String chunk) {
		String current = chunk;
		for (Pattern p : PATTERNS) {
			current = p.matcher(current).replaceAll(REDACTED);
		}
		return current;
	}
}
