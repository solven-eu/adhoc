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
package eu.solven.adhoc.js.webflux;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.resource.NoResourceFoundException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.app.AdhocJackson;
import eu.solven.adhoc.security.AccountForbiddenOperation;
import eu.solven.adhoc.security.LoginRouteButNotAuthenticatedException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Convert an applicative {@link Throwable} into a relevant {@link HttpStatus}
 * 
 * @author Benoit Lacelle
 *
 */
@Component
// '-2' to have higher priority than the default WebExceptionHandler
@Order(-2)
@Slf4j
// https://stackoverflow.com/questions/51931178/error-handling-in-webflux-with-routerfunction
public class AdhocWebExceptionHandler implements WebExceptionHandler {

	final ObjectMapper objectMapper = AdhocJackson.objectMapper();

	@Override
	public @NonNull Mono<Void> handle(@NonNull ServerWebExchange exchange, @NonNull Throwable e) {
		if (e instanceof NoResourceFoundException) {
			// Let the default WebExceptionHandler manage 404
			return Mono.error(e);
		}

		HttpStatus httpStatus;
		if (e instanceof LoginRouteButNotAuthenticatedException) {
			httpStatus = HttpStatus.UNAUTHORIZED;
		} else if (e instanceof AccountForbiddenOperation) {
			httpStatus = HttpStatus.FORBIDDEN;
		} else if (e instanceof IllegalArgumentException) {
			httpStatus = HttpStatus.BAD_REQUEST;
		} else {
			httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
		}

		ServerHttpResponse response = exchange.getResponse();
		response.setStatusCode(httpStatus);
		if (log.isDebugEnabled()) {
			log.warn("Returning a {} given {} ({})", httpStatus, e.getClass(), e.getMessage(), e);
		} else {
			log.warn("Returning a {} given {} ({})", httpStatus, e.getClass(), e.getMessage());
		}

		Map<String, Object> responseBody = new LinkedHashMap<>();

		if (e.getMessage() == null) {
			responseBody.put("error_message", "");
		} else {
			responseBody.put("error_message", e.getMessage());
		}

		String respondyBodyAsString;
		try {
			respondyBodyAsString = objectMapper.writeValueAsString(responseBody);
		} catch (JsonProcessingException ee) {
			log.error("Issue producing responseBody given {}", responseBody, ee);
			respondyBodyAsString = "{\"error_message\":\"something_went_very_wrong\"}";
		}

		byte[] bytes = respondyBodyAsString.getBytes(StandardCharsets.UTF_8);

		response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
		DataBuffer buffer = response.bufferFactory().wrap(bytes);
		return response.writeWith(Flux.just(buffer));
	}

}