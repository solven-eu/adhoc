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
package eu.solven.adhoc.pivotable.webmvc;

import java.time.Duration;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.CacheControl;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Servlet-stack counterpart of {@code PivotableWebjarsCachingWebFluxConfigurer}. Applies a far-future immutable cache
 * policy to every `/webjars/**` resource.
 *
 * See the reactive variant's Javadoc for the full rationale and the versionless-URL caveat: the two configurers are
 * deliberately kept in lock-step so both servers expose the same caching contract.
 * 
 * @author Benoit Lacelle
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class PivotableWebjarsCachingWebmvcConfigurer implements WebMvcConfigurer {
	private static final int DAYS_IN_YEAR = 365;

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/webjars/**")
				.addResourceLocations("classpath:/META-INF/resources/webjars/")
				.setCacheControl(CacheControl.maxAge(Duration.ofDays(DAYS_IN_YEAR)).cachePublic().immutable());
	}

}
