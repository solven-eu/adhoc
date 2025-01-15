package eu.solven.adhoc.js.webflux.api;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class QueryRequestHandler {
	final QueryExecutor queryExecutor;

	public Mono<ServerResponse> listGames(ServerRequest request) {
		Adh parameters = GameSearchParameters.builder();

		// https://stackoverflow.com/questions/24059773/correct-way-to-pass-multiple-values-for-same-parameter-name-in-get-request
		// `tag=a&tag=b` means we are looking for `a AND b`
		// `tag=a,b` means we are looking for `a OR b`
		// `tag=a,b&tag=c` means we are looking for `(a AND b) OR c`
		List<String> tags = request.queryParams().get("filter");
		if (tags != null) {
			tags.forEach(tag -> parameters.requiredTag(tag));
		}

		List<GameMetadata> games = queryExecutor.executeQuery(parameters.build());
		log.debug("Games for {}: {}", parameters, games);
		return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(games));
	}
}