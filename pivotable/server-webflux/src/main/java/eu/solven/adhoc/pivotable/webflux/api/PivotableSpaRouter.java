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

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Redirect the SinglePageApplication routes to index.html content.
 *
 * The main design is that all routes to `/api/*` are redirected to the backend, while `/html/*` routes are redirected
 * to the `HTML` and its own (SPA) routing logic.
 *
 * Minification and CDN-vs-WebJars swapping are fully client-side: the bootstrap block in {@code index.html} resolves
 * four importmap JSON files based on {@code ?cdn} and {@code ?min} query parameters. See that file for the contract.
 *
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class PivotableSpaRouter {

	@Value("classpath:/static/index.html")
	Resource indexHtml;

	// https://github.com/springdoc/springdoc-openapi-demos/tree/2.x/springdoc-openapi-spring-boot-2-webflux-functional
	// https://stackoverflow.com/questions/6845772/should-i-use-singular-or-plural-name-convention-for-rest-resources
	@Bean
	public RouterFunction<ServerResponse> spaRoutes(Environment env) {
		if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_HEROKU))) {
			log.info("We should rely on PRD resources in `index.html`");
		}

		Mono<ServerResponse> responseIndexHtml =
				ServerResponse.ok().contentType(MediaType.TEXT_HTML).bodyValue(indexHtml);

		return SpringdocRouteBuilder.route()

				// The following routes are useful for the SinglePageApplication
				.GET(html("/html/**"), request -> responseIndexHtml, ops -> ops.operationId("spaToRoute"))
				.GET(html("/login"), request -> responseIndexHtml, ops -> ops.operationId("spaToLogin"))

				.build();
	}

	private RequestPredicate html(String route) {
		return RequestPredicates.path(route).and(RequestPredicates.accept(MediaType.TEXT_HTML));
	}
}