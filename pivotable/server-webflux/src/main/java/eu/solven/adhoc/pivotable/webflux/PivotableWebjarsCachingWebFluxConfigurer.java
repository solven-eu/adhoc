/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.pivotable.webflux;

import java.time.Duration;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.CacheControl;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Apply a far-future immutable cache policy to every `/webjars/**` resource.
 *
 * WebJar artifacts are deployed with their version baked into the file name on the classpath
 * (`META-INF/resources/webjars/<artifact>/<version>/<file>`), so a given version's bytes never change. When the browser
 * requests a URL that embeds the version, the response is safe to cache for arbitrarily long. Set `public`,
 * `max-age=31536000` (one year) and `immutable` so modern browsers skip the revalidation round-trip entirely.
 *
 * Caveat: the SPA's `index.html` currently resolves WebJar assets via the versionless `/webjars/<artifact>/<path>`
 * shape (rewritten by `webjars-locator-core` at request time). With the long cache below, a WebJar version bump will
 * not propagate to already-visited clients until their cache expires — expected for production rollouts, but noticeable
 * during local dev. The eventual fix is to embed the version in the URL (either at build time via Maven filtering of
 * `index.html`, or by switching the SPA to the CDN-style `/webjars/<groupId>/<artifact>/ <version>/...` shape).
 *
 * Registered at {@link Ordered#HIGHEST_PRECEDENCE} so it takes precedence over Spring Boot's auto-configured
 * `/webjars/**` handler, which uses the default (short) cache.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class PivotableWebjarsCachingWebFluxConfigurer implements WebFluxConfigurer {

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/webjars/**")
				.addResourceLocations("classpath:/META-INF/resources/webjars/")
				.setCacheControl(CacheControl.maxAge(Duration.ofDays(365)).cachePublic().immutable());
	}

}
