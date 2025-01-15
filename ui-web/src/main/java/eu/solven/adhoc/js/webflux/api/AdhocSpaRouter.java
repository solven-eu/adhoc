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
package eu.solven.adhoc.js.webflux.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.kumite.app.IKumiteSpringProfiles;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Redirect the SinglePageApplication routes to index.html content.
 * 
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class AdhocSpaRouter {

	@Value("classpath:/static/index.html")
	Resource indexHtml;

	// https://github.com/springdoc/springdoc-openapi-demos/tree/2.x/springdoc-openapi-spring-boot-2-webflux-functional
	// https://stackoverflow.com/questions/6845772/should-i-use-singular-or-plural-name-convention-for-rest-resources
	@Bean
	public RouterFunction<ServerResponse> spaRoutes(Environment env) {
		if (env.acceptsProfiles(Profiles.of(IKumiteSpringProfiles.P_HEROKU))) {
			log.info("We should rely on PRD resources in `index.html`");
		}

		Resource filteredIndexHtml = filterIndexHtmlMl(env, indexHtml);

		Mono<ServerResponse> responseIndexHtml =
				ServerResponse.ok().contentType(MediaType.TEXT_HTML).bodyValue(filteredIndexHtml);

		return SpringdocRouteBuilder.route()

				// The following routes are useful for the SinglePageApplication
				.GET(html("/html/**"), request -> responseIndexHtml, ops -> ops.operationId("spaToRoute"))
				.GET(html("/login"), request -> responseIndexHtml, ops -> ops.operationId("spaToLogin"))

				.build();
	}

	private RequestPredicate html(String route) {
		return RequestPredicates.path(route).and(RequestPredicates.accept(MediaType.TEXT_HTML));
	}

	private Resource filterIndexHtmlMl(Environment env, Resource indexHtmlResource) {
		if (env.acceptsProfiles(Profiles.of(IKumiteSpringProfiles.P_PRDMODE))) {
			String indexHtml;
			try {
				indexHtml = indexHtmlResource.getContentAsString(StandardCharsets.UTF_8);
			} catch (IOException e) {
				throw new IllegalStateException("Issue loading " + indexHtmlResource, e);
			}

			indexHtml = minifyHtml(indexHtml);

			String fileName = indexHtmlResource.getFilename();
			log.info("{} has been minified", fileName);

			return new ByteArrayResource(indexHtml.getBytes(StandardCharsets.UTF_8),
					"fileName <minified for %s>".formatted(IKumiteSpringProfiles.P_PRDMODE));
		} else {
			return indexHtmlResource;
		}
	}

	/**
	 * This minification consists essentially in referring to minified external dependencies.
	 * 
	 * @param indexHtml
	 * @return a minified version of index.html
	 */
	String minifyHtml(String indexHtml) {
		String minified = indexHtml;

		minified = minified.replace("/bootstrap.css", "/bootstrap.min.css");
		minified = minified.replace("/bootstrap-icons.css", "/bootstrap-icons.min.css");

		minified = minified.replace("/vue.esm-browser.js", "/vue.esm-browser.prod.js");
		// https://github.com/vuejs/router/issues/694
		// minified = minified.replace("/vue-router.esm-browser.js", "/vue-router.esm-browser.???.js");

		minified = minified.replace("/bootstrap.esm.js", "/bootstrap.esm.min.js");

		// https://unpkg.com/@vue/devtools-api@6.2.1/lib/esm/index.js
		minified = minified.replace("/lib/esm/index.js", "/lib/esm/index.js");
		// https://unpkg.com/@popperjs/core@2.11.8/dist/esm/index.js"
		minified = minified.replace("/dist/esm/index.js", "/dist/esm/index.js");

		// No minified Pinia ESM?
		// minified = minified.replace("/pinia.esm-browser.js", "/pinia.esm-browser.min.js");

		minified = minified.replace("/vue-demi/lib/v3/index.mjs", "/vue-demi/lib/v3/index.min.mjs");
		minified = minified.replace("/vue.esm-browser.js", "/vue.esm-browser.prod.js");

		return minified;
	}
}