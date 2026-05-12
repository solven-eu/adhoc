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

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * Typed configuration for the Pivotable AI chat assistant, bound from the {@code adhoc.pivotable.chat.*} property tree.
 * Replaces the prior per-field {@code @Value("${...}")} injections in both web stacks so a single
 * {@code PivotableChatProperties} bean carries every chat knob.
 *
 * <p>
 * Canonical YAML shape:
 *
 * <pre>{@code
 * adhoc:
 *   pivotable:
 *     chat:
 *       anthropic-api-key: ${ANTHROPIC_API_KEY}    # required to activate the endpoint
 *       model: claude-haiku-4-5                    # default; override with claude-sonnet-4-6 etc.
 *       force-tool-call: false                     # true → Anthropic must end every turn with a tool call
 *       style:
 *         max-sentences: 2                         # hard cap on the model's plain-text reply length
 *         ambiguity: BEST_GUESS                    # or CLARIFY to keep clarifying questions
 * }</pre>
 *
 * <p>
 * The same keys work through Spring's relaxed-binding env-var form ({@code ADHOC_PIVOTABLE_CHAT_ANTHROPIC_API_KEY},
 * etc.) but YAML is the preferred shape — easier to review and to diff in PRs.
 *
 * @author Benoit Lacelle
 */
@ConfigurationProperties("adhoc.pivotable.chat")
@Data
public class PivotableChatProperties {

	/**
	 * Anthropic credential — either an {@code sk-ant-api03-…} API key or an {@code sk-ant-oat01-…} OAuth token from
	 * {@code claude setup-token}. Mandatory: leaving it unset deactivates the chat endpoint entirely (the
	 * {@code @ConditionalOnProperty} gate trips).
	 */
	private String anthropicApiKey;

	/** Anthropic model identifier. Default {@code claude-haiku-4-5} — cheapest with good tool-call accuracy. */
	private String model = "claude-haiku-4-5";

	/**
	 * When {@code true}, set Anthropic's {@code tool_choice: {"type":"any"}} so the model is required to end every turn
	 * with a tool call. Disables clarifying-question style answers.
	 */
	private boolean forceToolCall;

	/** Verbosity + ambiguity knobs woven into the system prompt. */
	private final Style style = new Style();

	/**
	 * Convenience: render the {@link #style} fields into the immutable {@link ChatStyle} value used by
	 * {@link ChatRequestPlanner}.
	 *
	 * @return a {@link ChatStyle} mirroring the current {@link #style} settings
	 */
	public ChatStyle toChatStyle() {
		return ChatStyle.builder().maxSentences(style.getMaxSentences()).ambiguity(style.getAmbiguity()).build();
	}

	/** Nested {@code adhoc.pivotable.chat.style.*} keys. Mirrors the immutable {@link ChatStyle} value object. */
	@Data
	public static class Style {

		/** Hard cap on the number of sentences the model is allowed in its plain-text reply. */
		private int maxSentences = 2;

		/** How to handle ambiguous requests. See {@link ChatStyle.Ambiguity}. */
		private ChatStyle.Ambiguity ambiguity = ChatStyle.Ambiguity.BEST_GUESS;
	}
}
