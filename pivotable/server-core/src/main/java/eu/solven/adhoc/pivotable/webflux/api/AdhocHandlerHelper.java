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

import java.util.Optional;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import reactor.core.publisher.Mono;

/**
 * Helpers around {@link AdhocApiRouter} handlers.
 * 
 * @author Benoit Lacelle
 *
 */
public class AdhocHandlerHelper {

	public static UUID uuid(String rawUuid, String idKey) {
		if ("undefined".equals(rawUuid)) {
			throw new IllegalArgumentException("`undefined` is an invalid `%s`".formatted(idKey));
		} else if (rawUuid == null) {
			throw new IllegalArgumentException("`null` is an invalid `%s`".formatted(idKey));
		}
		return UUID.fromString(rawUuid);
	}

	public static UUID uuid(ServerRequest request, String idKey) {
		Optional<String> optPlayerId = request.queryParam(idKey);
		return uuid(optPlayerId.orElseThrow(() -> new IllegalArgumentException("Lack `%s`".formatted(idKey))), idKey);
	}

	public static Optional<UUID> optUuid(ServerRequest request, String idKey) {
		Optional<String> optUuid = request.queryParam(idKey);

		return optUuid.map(rawUuid -> uuid(rawUuid, idKey));
	}

	public static Optional<String> optString(ServerRequest request, String idKey) {
		Optional<String> optUuid = request.queryParam(idKey);

		return optUuid.map(rawUuid -> string(rawUuid, idKey));
	}

	public static Optional<UUID> optUuid(Optional<String> optRaw, String idKey) {
		return optRaw.map(raw -> uuid(raw, idKey));
	}

	public static String string(ServerRequest request, String idKey) {
		Optional<String> optPlayerId = request.queryParam(idKey);
		return string(optPlayerId.orElseThrow(() -> new IllegalArgumentException("Lack `%s`".formatted(idKey))), idKey);
	}

	private static String string(String rawValue, String idKey) {
		if ("undefined".equals(rawValue)) {
			throw new IllegalArgumentException("`undefined` is an invalid `%s`".formatted(idKey));
		} else if (rawValue == null) {
			throw new IllegalArgumentException("`null` is an invalid `%s`".formatted(idKey));
		} else if (rawValue.isBlank()) {
			throw new IllegalArgumentException("`%s` is an invalid `%s`".formatted(rawValue, idKey));
		}
		return rawValue;
	}

	public static Optional<Boolean> optBoolean(ServerRequest request, String idKey) {
		Optional<String> optBoolean = request.queryParam(idKey);

		return optBoolean.map(rawBoolean -> {
			if ("undefined".equals(rawBoolean)) {
				throw new IllegalArgumentException("`undefined` is an invalid rawBoolean");
			}

			return Boolean.parseBoolean(rawBoolean);
		});
	}

	public static Mono<ServerResponse> okAsJson(Object body) {
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(body));
	}

	public static Optional<Number> optNumber(ServerRequest request, String idKey) {
		Optional<String> optDouble = request.queryParam(idKey);

		return optDouble.map(rawDouble -> {
			if ("undefined".equals(rawDouble)) {
				throw new IllegalArgumentException("`undefined` is an invalid rawDouble");
			}

			return Double.parseDouble(rawDouble);
		});
	}

	// public static Mono<ServerResponse> resourceGone(Map<String, ?> body) {
	// return ServerResponse.status(HttpStatus.NOT_FOUND)
	// .contentType(MediaType.APPLICATION_JSON)
	// .body(BodyInserters.fromValue(body));
	// }

}