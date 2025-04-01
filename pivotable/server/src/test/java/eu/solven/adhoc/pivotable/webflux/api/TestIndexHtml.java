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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
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

	@Autowired
	PivotableSpaRouter spaRouter;

	@Test
	public void testIndexHtml() throws IOException {
		String html = spaRouter.indexHtml.getContentAsString(StandardCharsets.UTF_8);
		checkHtmlForUrls(html);
	}

	@Test
	public void testMinifiedIndexHtml() throws IOException {
		String html = spaRouter.minifyHtml(spaRouter.indexHtml.getContentAsString(StandardCharsets.UTF_8));
		checkHtmlForUrls(html);
	}

	private void checkHtmlForUrls(String html) throws IOException {
		Document jsoup = Jsoup.parse(html);

		AtomicInteger nbChecked = new AtomicInteger();

		jsoup.getElementsByAttribute("href").forEach(linkElement -> {
			String href = linkElement.attr("href");

			checkUrl(nbChecked, href);
		});

		jsoup.getElementsByAttributeValue("type", "importmap").forEach(importMap -> {
			String script = importMap.data();

			Map<String, Map<String, String>> asMap;
			try {
				asMap = new ObjectMapper().readValue(script, Map.class);
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
