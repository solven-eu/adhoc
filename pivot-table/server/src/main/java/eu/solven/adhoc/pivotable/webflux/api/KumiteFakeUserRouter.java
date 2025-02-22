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
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import lombok.extern.slf4j.Slf4j;

/**
 * In case of fakeUser, we may want to enable addition routes, like resetServer for integration-tests purposes.
 * 
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
@Profile(IPivotableSpringProfiles.P_FAKEUSER + " & " + IPivotableSpringProfiles.P_INMEMORY)
@Import({

		KumiteClearHandler.class

})
public class KumiteFakeUserRouter {

	private RequestPredicate json(String route) {
		return RequestPredicates.path(route).and(RequestPredicates.accept(MediaType.APPLICATION_JSON));
	}

	@Bean
	public RouterFunction<ServerResponse> fakeUserRoutes(KumiteClearHandler kumiteResetHandler) {
		return SpringdocRouteBuilder.route()
				.POST(json("/api/v1/clear"),
						kumiteResetHandler::clear,
						ops -> ops.operationId("clear")
								.response(responseBuilder().responseCode("200").description("Cleared")))
				// One can clear with GET to make it easier for a human
				.GET(json("/api/v1/clear"),
						kumiteResetHandler::clear,
						ops -> ops.operationId("clear")
								.response(responseBuilder().responseCode("200").description("Cleared")))

				.build();

	}
}