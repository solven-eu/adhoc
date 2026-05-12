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

import java.net.http.HttpClient;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;

import eu.solven.adhoc.pivotable.chat.ChatAvailabilityGuard;
import eu.solven.adhoc.pivotable.chat.ChatRateLimiter;
import eu.solven.adhoc.pivotable.chat.PivotableChatProperties;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Wires the chat endpoints into the WebMVC server. Mirrors the WebFlux config: the controller (and its routes) are
 * <strong>always</strong> registered — whether or not {@code adhoc.pivotable.chat.anthropic-api-key} is set, and
 * regardless of the {@code adhoc.pivotable.chat.enabled} toggle. The controller itself resolves the current state on
 * every request and reports it via the JSON body of {@code GET /api/v1/cubes/chat/enabled}.
 *
 * @author Benoit Lacelle
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PivotableChatProperties.class)
@Slf4j
public class PivotableChatConfiguration {

	@Bean
	public PivotableChatController pivotableChatController(PivotableSchemaRegistry schemasRegistry,
			ObjectMapper objectMapper,
			AsyncTaskExecutor applicationTaskExecutor,
			PivotableChatProperties properties) {

		HttpClient httpClient = HttpClient.newBuilder().build();

		String apiKey = properties.getAnthropicApiKey();
		if (apiKey == null || apiKey.isBlank()) {
			log.info(
					"Pivotable chat (webmvc) mounted but NOT_CONFIGURED — set adhoc.pivotable.chat.anthropic-api-key to enable");
		} else if (!properties.isEnabled()) {
			log.info(
					"Pivotable chat (webmvc) mounted but DISABLED_BY_CONFIG — set adhoc.pivotable.chat.enabled=true to activate");
		} else {
			log.info("Pivotable chat (webmvc) enabled with model={} forceToolCall={} style={}",
					properties.getModel(),
					properties.isForceToolCall(),
					properties.getStyle());
		}
		return new PivotableChatController(schemasRegistry,
				objectMapper,
				httpClient,
				applicationTaskExecutor,
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
}
