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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
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
 * Minification and CDN-vs-WebJars swapping are fully client-side: the bootstrap block in {@code index.html} resolves
 * four importmap JSON files based on {@code ?cdn} and {@code ?min} query parameters. See that file for the contract.
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

	@PostConstruct
	public void init() {
		if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_HEROKU))) {
			log.info("We should rely on PRD resources in `index.html`");
		}
	}

	/**
	 * Serve the SPA shell for all HTML sub-routes so the Vue router can take over.
	 *
	 * @return the raw {@code index.html} resource. The bootstrap block in the file resolves asset URLs client-side.
	 */
	@ResponseBody
	@GetMapping(value = { "/html/**", "/login" }, produces = MediaType.TEXT_HTML_VALUE)
	public ResponseEntity<Resource> spa() {
		return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(indexHtml);
	}
}
