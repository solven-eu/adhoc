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

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * Activates the AI chat endpoint when {@code adhoc.pivotable.chat.anthropic-api-key} is set.
 *
 * <p>
 * Example configuration:
 * 
 * <pre>{@code
 * adhoc:
 *   pivotable:
 *     chat:
 *       anthropic-api-key: ${ANTHROPIC_API_KEY}
 *       model: claude-haiku-4-5-20251001   # optional, this is the default
 * }</pre>
 *
 * @author Benoit Lacelle
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("adhoc.pivotable.chat.anthropic-api-key")
@Slf4j
public class PivotableChatConfiguration {

	@Bean
	public PivotableChatHandler pivotableChatHandler(PivotableAdhocSchemaRegistry schemasRegistry,
			ObjectMapper objectMapper,
			WebClient.Builder webClientBuilder,
			@Value("${adhoc.pivotable.chat.anthropic-api-key}") String apiKey,
			@Value("${adhoc.pivotable.chat.model:claude-haiku-4-5-20251001}") String model) {

		WebClient anthropicClient = webClientBuilder.clone()
				.baseUrl("https://api.anthropic.com")
				.defaultHeader("x-api-key", apiKey)
				.defaultHeader("anthropic-version", "2023-06-01")
				.build();

		log.info("Pivotable chat enabled with model={}", model);
		return new PivotableChatHandler(schemasRegistry, objectMapper, anthropicClient, model);
	}

	@Bean
	public RouterFunction<ServerResponse> chatRoutes(PivotableChatHandler chatHandler) {
		String base = IPivotableApiConstants.PREFIX + "/cubes/chat";
		return route(GET(base + "/enabled"), req -> ServerResponse.noContent().build())
				.andRoute(POST(base).and(accept(MediaType.APPLICATION_JSON)), chatHandler::chat);
	}
}
