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
package eu.solven.adhoc.pivotable.entrypoint;

import java.util.List;
import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.adhoc.beta.schema.AdhocSchemaForApi;
import eu.solven.adhoc.pivotable.webflux.api.AdhocHandlerHelper;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Handler related to {@link AdhocEntrypointMetadata} and related
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableEntrypointsHandler {
	final AdhocEntrypointsRegistry entrypointsRegistry;
	final AdhocSchemaForApi schemaForApi;

	private List<AdhocEntrypointMetadata> matchingEntrypoints(ServerRequest request) {
		AdhocEntrypointSearch.AdhocEntrypointSearchBuilder parameters = AdhocEntrypointSearch.builder();

		AdhocHandlerHelper.optUuid(request, "entrypoint_id").ifPresent(id -> parameters.entrypointId(Optional.of(id)));

		Optional<String> optKeyword = request.queryParam("keyword");
		optKeyword.ifPresent(rawKeyword -> parameters.keyword(Optional.of(rawKeyword)));

		List<AdhocEntrypointMetadata> entrypoints = entrypointsRegistry.search(parameters.build());
		log.debug("Entrypoints for {}: {}", parameters, entrypoints);
		return entrypoints;
	}

	public Mono<ServerResponse> listEntrypoints(ServerRequest request) {
		List<AdhocEntrypointMetadata> entrypoints = matchingEntrypoints(request);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(entrypoints));
	}

	public Mono<ServerResponse> entrypointSchema(ServerRequest request) {
		List<AdhocEntrypointMetadata> entrypoints = matchingEntrypoints(request);

		List<EntrypointSchema> schemas = entrypoints.stream().map(entrypoint -> {
			if (!"http://localhost:self".equals(entrypoint.getUrl())) {
				throw new NotYetImplementedException("%s".formatted(PepperLogHelper.getObjectAndClass(entrypoint)));
			}

			return EntrypointSchema.builder().entrypoint(entrypoint).schema(schemaForApi.getMetadata()).build();
		}).toList();

		log.debug("Schemas for {}: {}", entrypoints, schemas);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(schemas));
	}
}