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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Serves the Single Page Application's {@code index.html} for all SPA routes, allowing the Vue router to handle
 * client-side navigation.
 *
 * @author Benoit Lacelle
 */
@Controller
@RequiredArgsConstructor
@Slf4j
public class PivotableSpaController {

	@Value("classpath:/static/index.html")
	Resource indexHtml;

	final Environment env;

	private Resource filteredIndexHtml;

	/** Minify index.html once at startup when the {@code prdmode} profile is active. */
	@PostConstruct
	public void init() {
		if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_HEROKU))) {
			log.info("We should rely on PRD resources in `index.html`");
		}
		filteredIndexHtml = filterIndexHtml(env, indexHtml);
	}

	/**
	 * Serve the SPA shell for all HTML sub-routes so the Vue router can take over.
	 *
	 * @return the (optionally minified) {@code index.html} resource.
	 */
	@ResponseBody
	@GetMapping(value = { "/html/**", "/login" }, produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<Resource> spa() {
		return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(filteredIndexHtml);
	}

	private Resource filterIndexHtml(Environment environment, Resource indexHtmlResource) {
		if (environment.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_PRDMODE))) {
			String content;
			try {
				content = indexHtmlResource.getContentAsString(StandardCharsets.UTF_8);
			} catch (IOException e) {
				throw new IllegalStateException("Issue loading " + indexHtmlResource, e);
			}

			content = minifyHtml(content);

			String fileName = indexHtmlResource.getFilename();
			log.info("{} has been minified", fileName);

			return new ByteArrayResource(content.getBytes(StandardCharsets.UTF_8),
					"fileName <minified for %s>".formatted(IPivotableSpringProfiles.P_PRDMODE));
		} else {
			return indexHtmlResource;
		}
	}

	/**
	 * Replaces dev-mode asset references with their minified/production equivalents.
	 *
	 * @param indexHtml
	 *            raw HTML content
	 * @return minified HTML content
	 */
	protected String minifyHtml(String indexHtml) {
		String minified = indexHtml;

		minified = minified.replace("/bootstrap.css", "/bootstrap.min.css");
		minified = minified.replace("/bootstrap-icons.css", "/bootstrap-icons.min.css");

		minified = minified.replace("/vue.esm-browser.js", "/vue.esm-browser.prod.js");

		minified = minified.replace("/bootstrap.esm.js", "/bootstrap.esm.min.js");

		minified = minified.replace("/lib/esm/index.js", "/lib/esm/index.js");
		minified = minified.replace("/dist/esm/index.js", "/dist/esm/index.js");

		minified = minified.replace("/vue-demi/lib/v3/index.mjs", "/vue-demi/lib/v3/index.min.mjs");
		minified = minified.replace("/vue.esm-browser.js", "/vue.esm-browser.prod.js");

		return minified;
	}
}
