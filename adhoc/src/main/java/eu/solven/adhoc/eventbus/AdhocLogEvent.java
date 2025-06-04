/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.eventbus;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Typically used for unitTests, to check some debug/explain feature.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocLogEvent implements IAdhocEvent, IIsExplainable, IIsDebugable {
	@Default
	boolean explain = false;
	// Are these info relative to performance?
	// This is especially important for tests, not included logs which would generally change from one run to another
	@Default
	boolean performance = false;
	@Default
	boolean debug = false;
	@Default
	boolean warn = false;

	@NonNull
	String message;

	// Useful for event filtering
	@NonNull
	@Singular
	ImmutableSet<String> tags;

	@NonNull
	Object source;

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class AdhocLogEventBuilder {
		/**
		 * Accept a template like `c={}` or `c=%s` and parameters to be injected into the template.
		 * 
		 * @param template
		 * @param parameters
		 * @return
		 */
		public AdhocLogEventBuilder messageT(String template, Object... parameters) {
			// From SLF4J placeholder `{}` to `.formatted` `%s`
			String templateForFormatted = template.replaceAll(Pattern.quote("{}"), Matcher.quoteReplacement("%s"));
			return this.message(templateForFormatted.formatted(parameters));
		}
	}
}
