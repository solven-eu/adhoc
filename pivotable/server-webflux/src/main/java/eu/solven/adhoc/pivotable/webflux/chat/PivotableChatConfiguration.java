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

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.chat.ChatAvailabilityGuard;
import eu.solven.adhoc.pivotable.chat.ChatRateLimiter;
import eu.solven.adhoc.pivotable.chat.PivotableChatProperties;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Wires the chat endpoints into the WebFlux server. The routes (and the {@link PivotableChatHandler} bean) are
 * <strong>always</strong> registered — whether or not {@code adhoc.pivotable.chat.anthropic-api-key} is set, and
 * regardless of the {@code adhoc.pivotable.chat.enabled} toggle. The handler itself consults
 * {@link eu.solven.adhoc.pivotable.chat.ChatAvailability#resolve} at request time and reports the current state via the
 * JSON body of {@code GET /api/v1/cubes/chat/enabled}, so the SPA always sees a 200 response and can render a stable UI
 * regardless of server-side configuration.
 *
 * @author Benoit Lacelle
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PivotableChatProperties.class)
@Slf4j
public class PivotableChatConfiguration {

	@Bean
	public PivotableChatHandler pivotableChatHandler(PivotableSchemaRegistry schemasRegistry,
			ObjectMapper objectMapper,
			PivotableChatProperties properties) {

		// Build the WebClient unconditionally so the bean shape stays stable. When no API key is configured we still
		// construct the client (no auth headers) — the handler short-circuits with a 503 long before any HTTP call is
		// made, so the unauthenticated client never reaches Anthropic. Building it directly via `WebClient.builder()`
		// rather than injecting `WebClient.Builder` because that bean is only provided by `WebClientAutoConfiguration`
		// in some Pivotable-derived apps; the static builder is functionally equivalent for our use case.
		WebClient.Builder builder = WebClient.builder()
				.baseUrl("https://api.anthropic.com")
				.defaultHeader("anthropic-version", "2023-06-01");
		String apiKey = properties.getAnthropicApiKey();
		if (apiKey != null && !apiKey.isBlank()) {
			// `sk-ant-oat01-…` tokens (from `claude setup-token`) authenticate via OAuth Bearer; the regular
			// `sk-ant-api03-…` API keys use the legacy `x-api-key` header. Auto-detect from the prefix.
			if (apiKey.startsWith("sk-ant-oat")) {
				builder.defaultHeader("Authorization", "Bearer " + apiKey);
			} else {
				builder.defaultHeader("x-api-key", apiKey);
			}
		}
		WebClient anthropicClient = builder.build();

		if (apiKey == null || apiKey.isBlank()) {
			log.info(
					"Pivotable chat mounted but NOT_CONFIGURED — set adhoc.pivotable.chat.anthropic-api-key to enable");
		} else if (!properties.isEnabled()) {
			log.info(
					"Pivotable chat mounted but DISABLED_BY_CONFIG — set adhoc.pivotable.chat.enabled=true to activate");
		} else {
			log.info("Pivotable chat enabled with model={} forceToolCall={} style={}",
					properties.getModel(),
					properties.isForceToolCall(),
					properties.getStyle());
		}
		return new PivotableChatHandler(schemasRegistry,
				objectMapper,
				anthropicClient,
				properties,
				chatAvailabilityGuard(),
				chatRateLimiter());
	}

	@Bean
	public ChatAvailabilityGuard chatAvailabilityGuard() {
		return new ChatAvailabilityGuard();
	}

	@Bean
	public ChatRateLimiter chatRateLimiter() {
		return new ChatRateLimiter();
	}

	@Bean
	public RouterFunction<ServerResponse> chatRoutes(PivotableChatHandler chatHandler) {
		String base = IPivotableApiConstants.PREFIX + "/cubes/chat";
		return route(GET(base + "/enabled"), chatHandler::enabled)
				.andRoute(POST(base).and(accept(MediaType.APPLICATION_JSON)), chatHandler::chat);
	}
}
