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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.CacheControl;
import org.springframework.web.reactive.config.ResourceHandlerRegistration;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;

/**
 * Capture what the configurer wires into the {@link ResourceHandlerRegistry} and assert it targets `/webjars/**` with a
 * far-future immutable {@link CacheControl}.
 */
public class TestPivotableWebjarsCachingWebFluxConfigurer {

	@Test
	public void testCacheControlHeaderIsLongImmutablePublic() {
		ResourceHandlerRegistration registration = Mockito.mock(ResourceHandlerRegistration.class);
		Mockito.when(registration.addResourceLocations(Mockito.any(String[].class))).thenReturn(registration);
		Mockito.when(registration.setCacheControl(Mockito.any())).thenReturn(registration);

		ResourceHandlerRegistry registry = Mockito.mock(ResourceHandlerRegistry.class);
		Mockito.when(registry.addResourceHandler(Mockito.any(String[].class))).thenReturn(registration);

		new PivotableWebjarsCachingWebFluxConfigurer().addResourceHandlers(registry);

		ArgumentCaptor<String[]> patternsCaptor = ArgumentCaptor.forClass(String[].class);
		Mockito.verify(registry).addResourceHandler(patternsCaptor.capture());
		Assertions.assertThat(patternsCaptor.getValue()).containsExactly("/webjars/**");

		ArgumentCaptor<CacheControl> cacheCaptor = ArgumentCaptor.forClass(CacheControl.class);
		Mockito.verify(registration).setCacheControl(cacheCaptor.capture());
		CacheControl cc = cacheCaptor.getValue();

		// The Spring API only exposes the final header string — compose-checks rather than
		// exact-match so an upstream header-formatting tweak does not break the test.
		String header = cc.getHeaderValue();
		Assertions.assertThat(header)
				.contains("max-age=" + Duration.ofDays(365).toSeconds())
				.contains("public")
				.contains("immutable");
	}

}
