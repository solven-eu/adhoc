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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Store available {@link PivotableAdhocEndpointMetadata}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableEndpointsRegistry {
	// One day, we could register externalized games, interacting by API. It will be a way not to concentrate all Games
	// in this project.
	final Map<UUID, PivotableAdhocEndpointMetadata> idToEndpoint = new ConcurrentHashMap<>();

	public void registerEntrypoint(PivotableAdhocEndpointMetadata endpoint) {
		UUID endpointId = endpoint.getId();

		if (endpointId == null) {
			throw new IllegalArgumentException("Missing endpointId: " + endpoint);
		}

		PivotableAdhocEndpointMetadata alreadyIn = idToEndpoint.putIfAbsent(endpointId, endpoint);
		if (alreadyIn != null) {
			throw new IllegalArgumentException("endpointId already registered: " + endpoint);
		}
		log.info("Registering endpointId={} endpointName={}", endpointId, endpoint.getName());
	}

	public PivotableAdhocEndpointMetadata getEntrypoint(UUID endpointId) {
		PivotableAdhocEndpointMetadata endpoint = idToEndpoint.get(endpointId);
		if (endpoint == null) {
			throw new IllegalArgumentException("No endpoint registered for id=" + endpointId);
		}
		return endpoint;
	}

	public List<PivotableAdhocEndpointMetadata> search(AdhocEndpointSearch search) {
		Stream<PivotableAdhocEndpointMetadata> metaStream;

		if (search.getEndpointId().isPresent()) {
			UUID uuid = search.getEndpointId().get();
			metaStream = Optional.ofNullable(idToEndpoint.get(uuid)).stream();
		} else {
			metaStream = idToEndpoint.values().stream();
		}

		if (search.getKeyword().isPresent()) {
			String keyword = search.getKeyword().get();
			metaStream = metaStream.filter(g -> g.getId().toString().contains(keyword) || g.getName().contains(keyword)
					|| g.getUrl().contains(keyword));
		}

		return metaStream.collect(Collectors.toList());
	}

	public Stream<? extends PivotableAdhocEndpointMetadata> getEndpoints() {
		return idToEndpoint.values().stream();
	}
}
