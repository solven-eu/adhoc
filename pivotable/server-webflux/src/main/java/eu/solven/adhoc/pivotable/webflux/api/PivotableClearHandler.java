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

import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.util.IHasCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * API enabling clearing anything transient. Useful for integration tests.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableClearHandler {
	final ApplicationContext appContext;

	// Webflux
	public Mono<ServerResponse> clear(ServerRequest serverRequest) {
		return clear().flatMap(body -> ServerResponse.ok().body(BodyInserters.fromValue(body)));
	}

	// Webmvc
	@GetMapping
	public Mono<Map<String, ?>> clear() {
		Map<String, IHasCache> beansWithCache = appContext.getBeansOfType(IHasCache.class);

		beansWithCache.forEach((name, bean) -> {
			log.info("About to IHasCache.invalidateAll() over bean={} ({})", name, bean);
			bean.invalidateAll();
		});

		return Mono.just(Map.of("clear", true, "caches", beansWithCache.keySet()));
	}
}
