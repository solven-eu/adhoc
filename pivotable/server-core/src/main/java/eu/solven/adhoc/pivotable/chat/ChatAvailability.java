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

/**
 * JSON-friendly snapshot of the chat assistant's current availability, returned from
 * {@code GET /api/v1/cubes/chat/enabled}.
 *
 * <p>
 * Always-present {@code enabled} flag lets the SPA make a single, branch-free check; the optional {@code reason} and
 * {@code retryAfterSeconds} fields are present only when {@code enabled == false} and let the UI render a more specific
 * message (e.g. "Chat unavailable: out of credit until 14:23"). The JSON shape is intentionally stable across all
 * disabled-states so the SPA does not need to special-case HTTP status codes.
 *
 * <p>
 * The chat endpoint is now always registered regardless of whether {@code adhoc.pivotable.chat.anthropic-api-key} is
 * set — see {@code PivotableChatConfiguration} in each web stack — so the SPA's probe always succeeds with 200, and the
 * body field below distinguishes the four reachable states.
 *
 * @param enabled
 *            {@code true} when calling {@code POST /api/v1/cubes/chat} is expected to succeed
 * @param reason
 *            short machine identifier explaining why chat is disabled. One of {@link #REASON_NOT_CONFIGURED},
 *            {@link #REASON_DISABLED_BY_CONFIG}, {@link #REASON_COOLDOWN}. {@code null} when {@code enabled == true}.
 * @param retryAfterSeconds
 *            present only for {@link #REASON_COOLDOWN} — number of seconds until the cooldown window expires.
 *            {@code null} for the other states.
 * @author Benoit Lacelle
 */
// `LeftCurly` needed until Eclipse4.39?
@SuppressWarnings({"checkstyle:LeftCurly","checkstyle:RightCurly","checkstyle:LineLength","checkstyle:MagicNumber"})public record ChatAvailability(boolean enabled,String reason,Long retryAfterSeconds){

/** No API key (or OAuth token) is configured — the chat will never reach Anthropic until configuration changes. */
public static final String REASON_NOT_CONFIGURED="NOT_CONFIGURED";

/** API key is present but {@code adhoc.pivotable.chat.enabled=false} explicitly disables the assistant. */
public static final String REASON_DISABLED_BY_CONFIG="DISABLED_BY_CONFIG";

/** {@link ChatAvailabilityGuard} is in an active cooldown window after a long-term upstream failure. */
public static final String REASON_COOLDOWN="COOLDOWN";

/** Convert to the {@code Retry-After} header value, in seconds, or 0 when not in a cooldown state. */
public long retryAfterSecondsOrZero(){if(retryAfterSeconds==null){return 0L;}else{return retryAfterSeconds;}}

public static ChatAvailability ofEnabled(){return new ChatAvailability(true,null,null);}

public static ChatAvailability ofNotConfigured(){return new ChatAvailability(false,REASON_NOT_CONFIGURED,null);}

public static ChatAvailability ofDisabledByConfig(){return new ChatAvailability(false,REASON_DISABLED_BY_CONFIG,null);}

public static ChatAvailability ofCooldown(long retryAfterSeconds){return new ChatAvailability(false,REASON_COOLDOWN,retryAfterSeconds);}

/**
 * Resolve the current availability by combining static config (API key presence, explicit on/off toggle) with the
 * runtime cooldown maintained by {@link ChatAvailabilityGuard}. Cheap pure function — callers can invoke this on every
 * request without caching concerns.
 *
 * @param apiKey
 *            the configured Anthropic API key / OAuth token; {@code null} or blank means "not configured"
 * @param enabledByConfig
 *            mirrors {@code adhoc.pivotable.chat.enabled}; {@code false} disables chat even when the key is set
 * @param guard
 *            the runtime cooldown tracker (must not be {@code null})
 * @return the current {@link ChatAvailability} snapshot
 */
public static ChatAvailability resolve(String apiKey,boolean enabledByConfig,ChatAvailabilityGuard guard){if(apiKey==null||apiKey.isBlank()){return ofNotConfigured();}if(!enabledByConfig){return ofDisabledByConfig();}return guard.disabledUntil().map(until->{long retry=Math.max(0L,until.getEpochSecond()-System.currentTimeMillis()/1000L);return ofCooldown(retry);}).orElseGet(ChatAvailability::ofEnabled);}}
