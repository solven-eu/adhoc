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
package eu.solven.adhoc.pivotable.webmvc.chat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import eu.solven.adhoc.beta.schema.EndpointSchemaMetadata;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
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
import tools.jackson.databind.ObjectMapper;

/**
 * Handles {@code GET /cubes/chat/enabled} (availability probe — always 200 with a JSON body) and
 * {@code POST /cubes/chat} (SSE stream) on the WebMVC server. Mirrors the WebFlux {@code PivotableChatHandler}: routes
 * are always mounted, the controller resolves {@link ChatAvailability} per request and short-circuits with 503 when the
 * chat is not currently usable.
 *
 * <p>
 * The Anthropic call uses the JDK {@link HttpClient}; the streamed response is iterated line-by-line on an
 * {@link AsyncTaskExecutor} so the request thread is freed quickly.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX + "/cubes/chat")
public class PivotableChatController {

	private static final String ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";

	/** Idle timeout for the SSE response on the SPA side. 0 means "no timeout" but most browsers/proxies cap. */
	private static final long SSE_TIMEOUT_MS = 5L * 60L * 1000L;

	/** HTTP status family divisor — `status / HTTP_STATUS_FAMILY != 2` flags any non-2xx response. */
	private static final int HTTP_STATUS_FAMILY = 100;

	final PivotableSchemaRegistry schemasRegistry;
	final ObjectMapper objectMapper;
	final HttpClient httpClient;
	final AsyncTaskExecutor taskExecutor;
	final PivotableChatProperties properties;
	final ChatAvailabilityGuard guard;
	final ChatRateLimiter rateLimiter;
	final ChatRequestPlanner planner = new ChatRequestPlanner();

	@GetMapping("/enabled")
	public ResponseEntity<ChatAvailability> enabled() {
		return ResponseEntity.ok(currentAvailability());
	}

	@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public ResponseEntity<?> chat(@RequestBody ChatRequest chatRequest,
			jakarta.servlet.http.HttpServletRequest httpRequest) {
		// Short-circuit when chat is not currently usable — same JSON body the probe returns, with HTTP 503 +
		// Retry-After
		// so well-behaved clients back off cleanly.
		ChatAvailability availability = currentAvailability();
		if (!availability.enabled()) {
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
					.contentType(MediaType.APPLICATION_JSON)
					.header("Retry-After", Long.toString(availability.retryAfterSecondsOrZero()))
					.body(availability);
		}

		// Mechanism (3): per-principal rate limit. Falls back to remote IP when no auth principal is plumbed yet.
		String key;
		if (httpRequest.getUserPrincipal() == null) {
			key = httpRequest.getRemoteAddr();
		} else {
			key = httpRequest.getUserPrincipal().getName();
		}
		if (!rateLimiter.tryAcquire(key)) {
			log.warn("Chat rate limit exceeded for key={} ({} per {})",
					key,
					rateLimiter.getMaxPerWindow(),
					rateLimiter.getWindow());
			return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
					.header("Retry-After", Long.toString(rateLimiter.getWindow().getSeconds()))
					.build();
		}

		IAdhocSchema schema = schemasRegistry.getSchema(chatRequest.getEndpointId());
		IAdhocSchema.AdhocSchemaQuery schemaQuery =
				IAdhocSchema.AdhocSchemaQuery.builder().cube(Optional.of(chatRequest.getCube())).build();
		EndpointSchemaMetadata metadata = schema.getMetadata(schemaQuery, false);

		Map<String, Object> anthropicBody = planner.buildAnthropicBody(chatRequest,
				metadata,
				properties.getModel(),
				properties.isForceToolCall(),
				properties.toChatStyle());

		SseEmitter emitter = new SseEmitter(SSE_TIMEOUT_MS);
		taskExecutor.execute(() -> pumpAnthropic(anthropicBody, emitter));
		return ResponseEntity.ok(emitter);
	}

	/**
	 * Pure read of the current availability — combines config (API key presence + enabled toggle) with the runtime
	 * guard state. Cheap; no caching needed.
	 */
	protected ChatAvailability currentAvailability() {
		return ChatAvailability.resolve(properties.getAnthropicApiKey(), properties.isEnabled(), guard);
	}

	/**
	 * Synchronously call the Anthropic Messages streaming endpoint and forward translated events to the
	 * {@link SseEmitter}. Completes (or fails) the emitter exactly once, even on errors.
	 */
	protected void pumpAnthropic(Map<String, Object> anthropicBody, SseEmitter emitter) {
		AnthropicSseTranslator translator = new AnthropicSseTranslator(objectMapper);
		try {
			HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
					.uri(URI.create(ANTHROPIC_MESSAGES_URL))
					.header("anthropic-version", "2023-06-01")
					.header("content-type", "application/json")
					.header("accept", "text/event-stream")
					.POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(anthropicBody)));
			// `sk-ant-oat01-…` tokens (from `claude setup-token`) authenticate via OAuth Bearer; the regular
			// `sk-ant-api03-…` API keys use the legacy `x-api-key` header. Auto-detect from the prefix.
			String apiKey = properties.getAnthropicApiKey();
			if (apiKey.startsWith("sk-ant-oat")) {
				requestBuilder.header("Authorization", "Bearer " + apiKey);
			} else {
				requestBuilder.header("x-api-key", apiKey);
			}
			HttpRequest httpRequest = requestBuilder.build();

			HttpResponse<Stream<String>> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofLines());
			if (response.statusCode() / HTTP_STATUS_FAMILY != 2) {
				String body;
				try (Stream<String> lines = response.body()) {
					body = lines
							.reduce(new StringBuilder(),
									(sb, line) -> sb.append(line).append('\n'),
									StringBuilder::append)
							.toString();
				}
				guard.tripIfLongTermFailure(body);
				emitter.send(SseEmitter.event()
						.data(translator
								.errorEvent("Anthropic returned HTTP " + response.statusCode() + " — body: " + body),
								MediaType.APPLICATION_JSON));
				emitter.complete();
				return;
			}

			try (Stream<String> lines = response.body()) {
				lines.forEach(line -> {
					if (line.startsWith("data:")) {
						String data = line.substring("data:".length()).stripLeading();
						translator.onAnthropicEvent(data, json -> sendData(emitter, json));
					}
				});
			}
			emitter.complete();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			emitter.completeWithError(e);
		} catch (IOException | RuntimeException e) {
			log.error("Anthropic stream error", e);
			try {
				emitter.send(
						SseEmitter.event().data(translator.errorEvent(e.getMessage()), MediaType.APPLICATION_JSON));
			} catch (IOException ignored) {
				// best-effort: the connection may already be gone
			}
			emitter.completeWithError(e);
		}
	}

	private void sendData(SseEmitter emitter, String json) {
		try {
			emitter.send(SseEmitter.event().data(json, MediaType.APPLICATION_JSON));
		} catch (IOException e) {
			throw new IllegalStateException("SSE emitter rejected event", e);
		}
	}
}
