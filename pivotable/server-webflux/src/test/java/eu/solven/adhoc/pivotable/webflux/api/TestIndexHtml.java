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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@Import(PivotableSpaRouter.class)
@Slf4j
public class TestIndexHtml {

	private static final String CDN_URL = "https://cdn.jsdelivr.net";

	/**
	 * Aborts all tests in this class if the external CDN is not reachable (e.g. corporate proxy blocking outbound
	 * HTTPS). The tests verify that every URL embedded in {@code index.html} returns a 2xx response; those checks would
	 * all fail with a connection error in a restricted network instead of giving a clear skip.
	 */
	@BeforeAll
	static void checkCdnConnectivity() {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(CDN_URL).openConnection();
			connection.setConnectTimeout(3000);
			connection.setReadTimeout(3000);
			connection.setRequestMethod("HEAD");
			connection.connect();
			connection.disconnect();
		} catch (IOException e) {
			Assumptions.assumeTrue(false,
					"CDN not reachable (%s): %s — skipping index.html URL checks".formatted(CDN_URL, e.getMessage()));
		}
	}

	@Autowired
	PivotableSpaRouter spaRouter;

	@Disabled("Import maps are not built dynamically (for webjar vs cdn); importmap JSONs live at "
			+ "/ui/importmap-{webjars,cdn}{,-min}.json and are checked by the JS unit-test suite.")
	@Test
	public void testIndexHtml() throws IOException {
		String html = spaRouter.indexHtml.getContentAsString(StandardCharsets.UTF_8);
		checkHtmlForUrls(html);
	}

	/**
	 * Smoke test — ensures index.html parses cleanly and exposes the structural anchors the SPA bootstrap depends on.
	 *
	 * Catches the kind of regression we hit when the literal text {@code <script>} appeared in inline-JS comments and
	 * Vite's HTML preprocessor mis-tokenised it, dropping half the document. Cheap and offline (no CDN required), so
	 * unlike {@link #testIndexHtml()} this one is left enabled.
	 */
	@Test
	public void testIndexHtml_structure() throws IOException {
		String html = spaRouter.indexHtml.getContentAsString(StandardCharsets.UTF_8);

		Document jsoup = Jsoup.parse(html);

		// Doctype + lang attribute survived the parse.
		Assertions.assertThat(jsoup.title()).isEqualTo("Pivotable (Adhoc)");
		Assertions.assertThat(jsoup.selectFirst("html").attr("lang")).isEqualTo("en");

		// SPA mount point. Without it Vue has nowhere to mount and the page stays blank.
		Assertions.assertThat(jsoup.selectFirst("#app")).as("#app mount point").isNotNull();

		// Bootstrap inline script + main module script are both present.
		Assertions.assertThat(jsoup.select("script"))
				.as("inline bootstrap + module script")
				.hasSizeGreaterThanOrEqualTo(2);
		Assertions.assertThat(jsoup.selectFirst("script[type=module]")).as("module script").isNotNull();

		// `?cdn` and `?dev` flags are wired in the bootstrap block — guard against an accidental rename.
		Assertions.assertThat(html).contains("params.has(\"cdn\")").contains("params.has(\"dev\")");

		// All four importmap JSONs are referenced from the bootstrap block.
		Assertions.assertThat(html)
				.contains("/ui/importmap-webjars.json")
				.contains("/ui/importmap-webjars-min.json")
				.contains("/ui/importmap-cdn.json")
				.contains("/ui/importmap-cdn-min.json");
	}

	private void checkHtmlForUrls(String html) throws IOException {
		Document jsoup = Jsoup.parse(html);

		AtomicInteger nbChecked = new AtomicInteger();

		jsoup.getElementsByAttribute("href").forEach(linkElement -> {
			String href = linkElement.attr("href");

			checkUrl(nbChecked, href);
		});

		ObjectMapper objectMapper = new ObjectMapper();
		jsoup.getElementsByAttributeValue("type", "importmap").forEach(importMap -> {
			String script = importMap.data();

			Map<String, Map<String, String>> asMap;
			try {
				asMap = objectMapper.readValue(script, Map.class);
			} catch (JsonProcessingException e) {
				throw new IllegalArgumentException("Invalid json: " + script);
			}

			asMap.get("imports").values().forEach(href -> {
				checkUrl(nbChecked, href);
			});
		});

		Assertions.assertThat(nbChecked.get()).isGreaterThan(5);
	}

	private void checkUrl(AtomicInteger nbChecked, String href) {
		if (href.startsWith("https://github.com/solven-eu/adhoc/commit/")) {
			log.debug("Skip the Github link to commit as current commit may not be pushed yet");
		} else if (href.startsWith("http")) {
			try {
				HttpURLConnection urlConnection = (HttpURLConnection) URI.create(href).toURL().openConnection();

				urlConnection.connect();

				int code = urlConnection.getResponseCode();
				// Check it is a 2XX
				Assertions.assertThat(code / 100).as(href).isEqualTo(2);
			} catch (IOException e) {
				throw new IllegalArgumentException("Invalid href: " + href, e);
			}

			log.info("href={} is valid", href);
			nbChecked.incrementAndGet();
		}
	}
}
