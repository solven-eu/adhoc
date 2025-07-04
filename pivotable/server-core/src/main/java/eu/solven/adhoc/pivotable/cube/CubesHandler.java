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
package eu.solven.adhoc.pivotable.cube;

import java.util.List;
import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Used?
 * 
 * @author Benoit Lacelle
 */
@Deprecated(since = "used?")
@RequiredArgsConstructor
@Slf4j
public class CubesHandler {
	final AdhocCubesRegistry cubesRegistry;

	public Mono<ServerResponse> listCubes(ServerRequest request) {
		PivotableCubeSearchParameters.PivotableCubeSearchParametersBuilder parameters =
				PivotableCubeSearchParameters.builder();

		AdhocHandlerHelper.optUuid(request, "endpoint_id").ifPresent(id -> parameters.endpointId(Optional.of(id)));
		AdhocHandlerHelper.optString(request, "cube").ifPresent(id -> parameters.cube(Optional.of(id)));

		Optional<String> optKeyword = request.queryParam("keyword");
		optKeyword.ifPresent(rawKeyword -> parameters.keyword(Optional.of(rawKeyword)));

		List<PivotableCubeMetadata> cubes = cubesRegistry.searchCubes(parameters.build());
		log.debug("Cubes for {}: {}", parameters, cubes);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(cubes));
	}
}