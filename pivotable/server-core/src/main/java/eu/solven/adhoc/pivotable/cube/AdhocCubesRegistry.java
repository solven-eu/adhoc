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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
@Deprecated(since = "used?")
public class AdhocCubesRegistry {
	// One day, we could register externalized games, interacting by API. It will be a way not to concentrate all Games
	// in this project.
	final Map<PivotableCubeId, PivotableCubeMetadata> idToCube = new ConcurrentHashMap<>();

	public void registerCube(PivotableCubeMetadata cube) {
		PivotableCubeId cubeId = cube.getId();

		if (cubeId == null) {
			throw new IllegalArgumentException("Missing cubeId: " + cube);
		}

		PivotableCubeMetadata alreadyIn = idToCube.putIfAbsent(cubeId, cube);
		if (alreadyIn != null) {
			throw new IllegalArgumentException("cubeId already registered: " + cube);
		}
		log.info("Registering cubeId={}", cubeId);
	}

	public PivotableCubeMetadata getCube(PivotableCubeId cubeId) {
		PivotableCubeMetadata cube = idToCube.get(cubeId);
		if (cube == null) {
			throw new IllegalArgumentException("No cube registered for id=" + cubeId);
		}
		return cube;
	}

	public List<PivotableCubeMetadata> searchCubes(PivotableCubeSearchParameters search) {
		Stream<PivotableCubeMetadata> metaStream;

		if (search.getEndpointId().isPresent() && search.getCube().isPresent()) {
			PivotableCubeId cubeId = PivotableCubeId.of(search.getEndpointId().get(), search.getCube().get());
			metaStream = Optional.ofNullable(idToCube.get(cubeId)).stream();
		} else {
			metaStream = idToCube.values().stream();
		}

		if (search.getKeyword().isPresent()) {
			String keyword = search.getKeyword().get();
			metaStream = metaStream.filter(
					g -> g.getId().getCube().contains(keyword) || g.getId().getEndpointId().toString().contains(keyword)
							|| g.getMeasures().stream().anyMatch(measure -> measure.contains(keyword)));
		}

		return metaStream.collect(Collectors.toList());
	}

	// public Stream<? extends AdhocEntrypointMetadata> getGames() {
	// return idToCube.values().stream();
	// }
}
