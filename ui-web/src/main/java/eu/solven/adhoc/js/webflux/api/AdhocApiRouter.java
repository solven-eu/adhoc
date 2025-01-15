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
package eu.solven.adhoc.js.webflux.api;

import static org.springdoc.core.fn.builders.apiresponse.Builder.responseBuilder;
import static org.springdoc.core.fn.builders.parameter.Builder.parameterBuilder;
import static org.springdoc.core.fn.builders.requestbody.Builder.requestBodyBuilder;

import org.springdoc.core.fn.builders.parameter.Builder;
import org.springdoc.webflux.core.fn.SpringdocRouteBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import eu.solven.kumite.account.internal.KumiteUser;
import eu.solven.kumite.app.webflux.PlayerVerifierFilterFunction;
import eu.solven.kumite.board.BoardHandler;
import eu.solven.kumite.contest.ContestHandler;
import eu.solven.kumite.contest.ContestMetadataRaw;
import eu.solven.kumite.contest.ContestView;
import eu.solven.kumite.game.GameMetadata;
import eu.solven.kumite.game.GameSearchHandler;
import eu.solven.kumite.leaderboard.LeaderboardHandler;
import eu.solven.kumite.move.IKumiteMove;
import eu.solven.kumite.player.KumitePlayer;
import eu.solven.kumite.player.PlayerContestStatus;
import eu.solven.kumite.player.PlayerMovesHandler;
import eu.solven.kumite.player.PlayerRawMovesHolder;
import eu.solven.kumite.player.PlayersSearchHandler;
import eu.solven.kumite.webhook.WebhooksHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * Redirect each route (e.g. `/games/someGameId`) to the appropriate handler.
 * 
 * @author Benoit Lacelle
 *
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class AdhocApiRouter {

	private static final RequestPredicate json(String path) {
		final RequestPredicate json = RequestPredicates.accept(MediaType.APPLICATION_JSON);
		return RequestPredicates.path("/api/v1" + path).and(json);
	}

	// https://github.com/springdoc/springdoc-openapi-demos/tree/2.x/springdoc-openapi-spring-boot-2-webflux-functional
	// https://stackoverflow.com/questions/6845772/should-i-use-singular-or-plural-name-convention-for-rest-resources
	@Bean
	public RouterFunction<ServerResponse> apiRoutes(
			WebhooksHandler webhooksHandler) {

		Builder accountId = parameterBuilder().name("account_id").description("Search for a specific accountId");
		Builder gameId = parameterBuilder().name("game_id").description("Search for a specific contestId");
		Builder playerId = parameterBuilder().name("player_id").description("Search for a specific playerId");
		Builder contestId = parameterBuilder().name("contest_id").description("Search for a specific contestId");

		return SpringdocRouteBuilder.route()
				.GET(json("/games"),
						gamesSearchHandler::listGames,
						ops -> ops.operationId("searchGames")
								.parameter(gameId)
								.parameter(parameterBuilder().name("min_players")
										.description("Search games with at most given minPlayers"))
								.parameter(parameterBuilder().name("max_players")
										.description("Search games with at least given maxPlayers"))
								.parameter(parameterBuilder().name("title_regex")
										.description("Search games with a title matching given Regex"))
								.response(
										responseBuilder().responseCode("200").implementationArray(GameMetadata.class)))

				.GET(json("/players"),
						playersSearchHandler::listPlayers,
						ops -> ops.operationId("searchPlayers")
								.parameter(playerId)
								.parameter(contestId)
								.parameter(accountId)
								.response(
										responseBuilder().responseCode("200").implementationArray(KumitePlayer.class)))

				.GET(json("/accounts"),
						playersSearchHandler::listPlayers,
						ops -> ops.operationId("searchAccounts")
								.parameter(accountId)
								.response(responseBuilder().responseCode("200").implementationArray(KumiteUser.class)))

				.GET(json("/contests"),
						contestSearchHandler::listContests,
						ops -> ops.operationId("searchContest")
								.parameter(gameId)
								.response(responseBuilder().responseCode("200")
										.implementationArray(ContestMetadataRaw.class)))
				.POST(json("/contests"),
						contestSearchHandler::openContest,
						ops -> ops.operationId("publishContest").parameter(gameId))
				.DELETE(json("/contests"),
						contestSearchHandler::openContest,
						ops -> ops.operationId("forceGameover").parameter(contestId))

				.GET(json("/board"),
						boardHandler::getBoard,
						ops -> ops.operationId("fetchBoard")
								.parameter(playerId)
								.parameter(contestId)
								.response(responseBuilder().implementation(ContestView.class)))
				.POST(json("/board/player"),
						playerMovesHandler::registerPlayer,
						ops -> ops.operationId("registerPlayer")
								.parameter(playerId)
								.parameter(contestId)
								.parameter(parameterBuilder().name("viewer")
										.description("`true` if you want to spectate the contest")
										.implementation(Boolean.class)
										.required(false))
								.response(responseBuilder().implementation(PlayerContestStatus.class)))
				.GET(json("/board/moves"),
						playerMovesHandler::listPlayerMoves,
						ops -> ops.operationId("fetchMoves")
								.parameter(playerId)
								.parameter(contestId)
								.response(responseBuilder().implementation(PlayerRawMovesHolder.class)))
				.POST(json("/board/move"),
						playerMovesHandler::playMove,
						ops -> ops.operationId("playMove")
								.parameter(playerId)
								.parameter(contestId)
								.requestBody(requestBodyBuilder().implementation(IKumiteMove.class)))

				.GET(json("/leaderboards"),
						leaderboardHandler::listScores,
						ops -> ops.operationId("fetchLeaderboard").parameter(contestId))

				// Activate webhooks later. For now, we focus on long-polling
				// .GET(json("/webhooks"),
				// webhooksHandler::listWebhooks,
				// ops -> ops.operationId("listWebhooks"))
				// .PUT(RequestPredicates.PUT("/webhooks"),
				// webhooksHandler::registerWebhook,
				// ops -> ops.operationId("publishWebhook"))
				// .DELETE(RequestPredicates.DELETE("/webhooks"),
				// webhooksHandler::dropWebhooks,
				// ops -> ops.operationId("deleteWebhook"))

				.build();

	}
}