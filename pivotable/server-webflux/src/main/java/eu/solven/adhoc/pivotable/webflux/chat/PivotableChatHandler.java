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
package eu.solven.adhoc.pivotable.webflux.chat;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.pivotable.chat.AnthropicSseTranslator;
import eu.solven.adhoc.pivotable.chat.ChatAvailability;
import eu.solven.adhoc.pivotable.chat.ChatAvailabilityGuard;
import eu.solven.adhoc.pivotable.chat.ChatRateLimiter;
import eu.solven.adhoc.pivotable.chat.ChatRequest;
import eu.solven.adhoc.pivotable.chat.ChatRequestPlanner;
import eu.solven.adhoc.pivotable.chat.PivotableChatProperties;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

/**
 * Handles GET {@code /cubes/chat/enabled} (always-available availability probe) and POST {@code /cubes/chat} (Anthropic
 * SSE proxy) for the WebFlux server. Both endpoints are always mounted; the handler consults
 * {@link ChatAvailability#resolve(String, boolean, ChatAvailabilityGuard)} at every request and short-circuits with a
 * 503 + {@link ChatAvailability} body when the chat is not currently usable.
 *
 * <p>
 * All non-transport logic (system prompt, message + tools assembly, Anthropic-to-simplified SSE translation) lives in
 * {@link ChatRequestPlanner} / {@link AnthropicSseTranslator} so it can be shared with the WebMVC controller.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableChatHandler {

	final PivotableSchemaRegistry schemasRegistry;
	final ObjectMapper objectMapper;
	final WebClient anthropicClient;
	final PivotableChatProperties properties;
	final ChatAvailabilityGuard guard;
	final ChatRateLimiter rateLimiter;
	final ChatRequestPlanner planner = new ChatRequestPlanner();

	/**
	 * Probe consumed by the SPA's chatbot on mount: always returns 200 with a JSON body describing the current
	 * availability. The endpoint is stable across all four reachable states (enabled / not configured / disabled by
	 * config / cooldown) so the SPA does not have to special-case HTTP status codes.
	 */
	public Mono<ServerResponse> enabled(ServerRequest request) {
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(currentAvailability());
	}

	public Mono<ServerResponse> chat(ServerRequest request) {
		// First: short-circuit with the same JSON body the probe returns whenever chat is not usable. The SPA already
		// hides its panel based on the probe, but a stray POST from a script or stale tab should still get a clean
		// 503 instead of a request that crashes deeper down on a null/blank API key.
		ChatAvailability availability = currentAvailability();
		if (!availability.enabled()) {
			return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
					.contentType(MediaType.APPLICATION_JSON)
					.header("Retry-After", Long.toString(availability.retryAfterSecondsOrZero()))
					.bodyValue(availability);
		}

		// Mechanism (3): per-principal sliding-window rate limit. Falls back to the remote IP when no auth principal is
		// attached to the exchange. 429 on overflow.
		return request.principal().map(Principal::getName).defaultIfEmpty(remoteIp(request)).flatMap(key -> {
			if (!rateLimiter.tryAcquire(key)) {
				log.warn("Chat rate limit exceeded for key={} ({} per {})",
						key,
						rateLimiter.getMaxPerWindow(),
						rateLimiter.getWindow());
				return ServerResponse.status(HttpStatus.TOO_MANY_REQUESTS)
						.header("Retry-After", Long.toString(rateLimiter.getWindow().getSeconds()))
						.build();
			}
			return doChat(request);
		});
	}

	/**
	 * Pure read of the current availability — combines config (API key presence + enabled toggle) with the runtime
	 * guard state. Cheap; no caching needed.
	 */
	protected ChatAvailability currentAvailability() {
		return ChatAvailability.resolve(properties.getAnthropicApiKey(), properties.isEnabled(), guard);
	}

	private Mono<ServerResponse> doChat(ServerRequest request) {
		return request.bodyToMono(ChatRequest.class).flatMap(chatRequest -> {
			IAdhocSchema schema = schemasRegistry.getSchema(chatRequest.getEndpointId());

			IAdhocSchema.AdhocSchemaQuery schemaQuery =
					IAdhocSchema.AdhocSchemaQuery.builder().cube(Optional.of(chatRequest.getCube())).build();
			EndpointSchemaMetadata metadata = schema.getMetadata(schemaQuery, false);

			Map<String, Object> anthropicBody = planner.buildAnthropicBody(chatRequest,
					metadata,
					properties.getModel(),
					properties.isForceToolCall(),
					properties.toChatStyle());

			Flux<String> sseFlux = callAnthropic(anthropicBody);

			return ServerResponse.ok().contentType(MediaType.TEXT_EVENT_STREAM).body(sseFlux, String.class);
		});
	}

	private static String remoteIp(ServerRequest request) {
		return extractRemoteIp(request.remoteAddress());
	}

	/**
	 * Pick a rate-limiter key from a remote socket address, robust against the {@code getAddress()=null} case (which
	 * happens with unresolved addresses on some Netty configurations and previously crashed every chat turn with NPE).
	 * Package-private so the unit test can exercise the three branches without mocking {@code ServerRequest}.
	 *
	 * @param remoteAddress
	 *            the request's remote address, may be empty
	 * @return the IP literal when resolvable, the host string when only that is available, or {@code "anonymous"} as a
	 *         last resort
	 */
	static String extractRemoteIp(Optional<java.net.InetSocketAddress> remoteAddress) {
		return remoteAddress.map(a -> {
			if (a.getAddress() != null) {
				return a.getAddress().getHostAddress();
			}
			if (a.getHostString() == null) {
				return "anonymous";
			}
			return a.getHostString();
		}).orElse("anonymous");
	}

	protected Flux<String> callAnthropic(Map<String, Object> body) {
		Flux<ServerSentEvent<String>> rawStream = anthropicClient.post()
				.uri("/v1/messages")
				.contentType(MediaType.APPLICATION_JSON)
				.accept(MediaType.TEXT_EVENT_STREAM)
				.bodyValue(body)
				.retrieve()
				.bodyToFlux(new ParameterizedTypeReference<>() {
				});

		return translateStream(rawStream);
	}

	/**
	 * Bridge the raw Anthropic SSE flux into the simplified SSE flux consumed by the SPA, framing each event with
	 * {@code data: ...\n\n}.
	 */
	protected Flux<String> translateStream(Flux<ServerSentEvent<String>> rawStream) {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		// Emit RAW JSON strings — Spring's WebFlux SSE codec wraps each Flux<String> emission with `data:` framing
		// automatically when the response content type is text/event-stream. Adding our own `data: ...\n\n` framing
		// on top produced a double-prefixed wire output (`data:data: {...}\n data:\n data:\n`) that the SPA parser
		// silently failed to JSON.parse, leaving the assistant bubble empty.
		return Flux.create(sink -> rawStream.subscribe(event -> {
			translator.onAnthropicEvent(event.data(), json -> {
				sink.next(json);
				// Anthropic sometimes holds the SSE connection open after `message_stop` instead of closing it
				// immediately. Without this explicit complete, the SPA's reader.read() loop blocks forever waiting
				// for {done:true}, so `isSending = false` in the finally block never runs and the Send button stays
				// greyed.
				if (json.contains("\"type\":\"done\"")) {
					sink.complete();
				}
			});
		}, error -> {
			String detail = describeError(error);
			log.error("Anthropic stream error: {}", detail, error);
			if (error instanceof WebClientResponseException wcre) {
				guard.tripIfLongTermFailure(wcre.getResponseBodyAsString());
			}
			sink.next(translator.errorEvent(detail));
			sink.error(error);
		}, sink::complete));
	}

	/**
	 * Build a human-readable error description that includes Anthropic's response body when the failure is an HTTP
	 * status error. The default {@code WebClientResponseException#getMessage()} only carries the status + URL, hiding
	 * the JSON {@code {"error":{"type":..., "message":...}}} payload that explains the real problem.
	 */
	protected String describeError(Throwable error) {
		if (error instanceof WebClientResponseException wcre) {
			String body = wcre.getResponseBodyAsString();
			return wcre.getStatusCode() + " from Anthropic — body: " + body;
		}
		if (error.getMessage() == null) {
			return error.getClass().getSimpleName();
		} else {
			return error.getMessage();
		}
	}
}
