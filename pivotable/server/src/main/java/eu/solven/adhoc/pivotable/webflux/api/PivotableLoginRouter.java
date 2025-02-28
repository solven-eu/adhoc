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
package eu.solven.adhoc.pivotable.webflux.api;

import static org.springdoc.core.fn.builders.apiresponse.Builder.responseBuilder;

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.pivotable.greeting.Greeting;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.security.tokens.AccessTokenHandler;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * Redirect each route (e.g. `/games/someGameId`) to the appropriate handler.
 * 
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class PivotableLoginRouter {

	private static final RequestPredicate json(String path) {
		final RequestPredicate json = RequestPredicates.accept(MediaType.APPLICATION_JSON);
		return RequestPredicates.path(IPivotableApiConstants.PREFIX + path).and(json);
	}

	@Bean
	public RouterFunction<ServerResponse> loginRoutes(
			// PlayerVerifierFilterFunction playerVerifierFilterFunction,
			GreetingHandler greetingHandler,
			AccessTokenHandler accessTokenHandler) {
		// Builder playerId = parameterBuilder().name("player_id").description("Search for a specific playerId");

		return SpringdocRouteBuilder.route()

				// These API are useful only to test the API
				.GET(json("/hello"),
						greetingHandler::hello,
						ops -> ops.operationId("hello").response(responseBuilder().implementation(Greeting.class)))
				.POST(json("/hello"),
						greetingHandler::hello,
						ops -> ops.operationId("hello").response(responseBuilder().implementation(Greeting.class)))

				// https://datatracker.ietf.org/doc/html/rfc6749#section-1.5
				// https://curity.io/resources/learn/oauth-refresh/
				// `/token` is the standard route to fetch tokens
				.GET(json("/oauth2/token"),
						accessTokenHandler::getAccessToken,
						ops -> ops.operationId("getAccessTokenFromRefreshToken")
								// .parameter(playerId)
								.response(responseBuilder().implementation(AccessTokenWrapper.class)))

				// .filter(playerVerifierFilterFunction, ops -> {
				// // https://github.com/springdoc/springdoc-openapi/issues/1538
				// ops.operationId("login");
				// })

				.build();

	}
}