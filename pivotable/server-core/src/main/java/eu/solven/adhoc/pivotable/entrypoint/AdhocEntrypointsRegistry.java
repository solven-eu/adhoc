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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class AdhocEntrypointsRegistry {
	// One day, we could register externalized games, interacting by API. It will be a way not to concentrate all Games
	// in this project.
	final Map<UUID, AdhocEntrypointMetadata> idToServer = new ConcurrentHashMap<>();

	public void registerGame(AdhocEntrypointMetadata game) {
		UUID serverId = game.getId();

		if (serverId == null) {
			throw new IllegalArgumentException("Missing gameId: " + game);
		}

		AdhocEntrypointMetadata alreadyIn = idToServer.putIfAbsent(serverId, game);
		if (alreadyIn != null) {
			throw new IllegalArgumentException("gameId already registered: " + game);
		}
		log.info("Registering serverId={} serverName={}", serverId, game.getName());
	}

	public AdhocEntrypointMetadata getGame(UUID gameId) {
		AdhocEntrypointMetadata game = idToServer.get(gameId);
		if (game == null) {
			throw new IllegalArgumentException("No game registered for id=" + gameId);
		}
		return game;
	}

	public List<AdhocEntrypointMetadata> searchGames(AdhocEntrypointSearchParameters search) {
		Stream<AdhocEntrypointMetadata> metaStream;

		if (search.getEntrypointId().isPresent()) {
			UUID uuid = search.getEntrypointId().get();
			metaStream = Optional.ofNullable(idToServer.get(uuid)).stream();
		} else {
			metaStream = idToServer.values().stream();
		}

		if (search.getKeyword().isPresent()) {
			String keyword = search.getKeyword().get();
			metaStream = metaStream.filter(g -> g.getId().toString().contains(keyword) || g.getName().contains(keyword)
					|| g.getUrl().contains(keyword));
		}

		return metaStream.collect(Collectors.toList());
	}

	public Stream<? extends AdhocEntrypointMetadata> getGames() {
		return idToServer.values().stream();
	}
}
