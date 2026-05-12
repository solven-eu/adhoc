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

import lombok.Builder;
import lombok.Value;

/**
 * Caller-tunable knobs that shape the system prompt sent to Anthropic. Lets a deployment trade verbosity vs.
 * conciseness and clarify-first vs. best-guess behaviour without code changes.
 *
 * <p>
 * Plumbed through
 * {@link ChatRequestPlanner#buildAnthropicBody(ChatRequest, eu.solven.adhoc.beta.schema.EndpointSchemaMetadata, String, boolean, ChatStyle)}.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class ChatStyle {

	/**
	 * How the model should react when the user's request doesn't unambiguously map to existing measures / columns.
	 */
	public enum Ambiguity {
		/**
		 * Pick the closest-named measure / column and call the tool. Optimises for momentum — the user sees something
		 * happen, can correct it with one follow-up. The recommended default.
		 */
		BEST_GUESS,
		/** Ask the user a one-sentence clarifying question before calling any tool. Optimises for precision. */
		CLARIFY
	}

	/** Hard cap on the number of sentences the model is allowed in its plain-text reply. */
	@Builder.Default
	int maxSentences = 2;

	/** How to handle ambiguous requests. See {@link Ambiguity}. */
	@Builder.Default
	Ambiguity ambiguity = Ambiguity.BEST_GUESS;

	/** @return the project-wide default style — synthetic answers with best-guess tool selection. */
	public static ChatStyle defaults() {
		return ChatStyle.builder().build();
	}
}
