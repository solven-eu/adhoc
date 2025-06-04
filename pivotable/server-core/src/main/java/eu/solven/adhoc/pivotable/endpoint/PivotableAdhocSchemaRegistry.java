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
package eu.solven.adhoc.pivotable.endpoint;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Stores available {@link AdhocSchema}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableAdhocSchemaRegistry {
	// One day, we could register externalized games, interacting by API. It will be a way not to concentrate all Games
	// in this project.
	final Map<UUID, AdhocSchema> idToSchema = new ConcurrentHashMap<>();

	public void registerEntrypoint(UUID endpointId, AdhocSchema schema) {
		if (endpointId == null) {
			throw new IllegalArgumentException("Missing endpointId: " + schema);
		}

		AdhocSchema alreadyIn = idToSchema.putIfAbsent(endpointId, schema);
		if (alreadyIn != null) {
			throw new IllegalArgumentException(
					"schema for endpointId=%s already registered: %s".formatted(endpointId, schema));
		}
		log.info("Registering schema for endpointId={}", endpointId);
	}

	public AdhocSchema getSchema(UUID endpointId) {
		AdhocSchema schema = idToSchema.get(endpointId);
		if (schema == null) {
			throw new IllegalArgumentException("No schema registered for id=" + endpointId);
		}
		return schema;
	}

	public List<AdhocSchema> search(AdhocEndpointSearch search) {
		Stream<AdhocSchema> metaStream;

		if (search.getEndpointId().isPresent()) {
			UUID uuid = search.getEndpointId().get();
			metaStream = Optional.ofNullable(idToSchema.get(uuid)).stream();
		} else {
			metaStream = idToSchema.values().stream();
		}

		if (search.getKeyword().isPresent()) {
			String keyword = search.getKeyword().get();
			metaStream = metaStream.filter(g -> g.toString().contains(keyword));
		}

		return metaStream.collect(Collectors.toList());
	}

	public Stream<? extends AdhocSchema> getSchemas() {
		return idToSchema.values().stream();
	}
}
