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
package eu.solven.adhoc.pivotable.app.it;

import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.app.PivotableContestServerApplication;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.EntrypointsHandler;
import eu.solven.adhoc.pivotable.greeting.Greeting;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.webflux.PivotableWebExceptionHandler;
import eu.solven.adhoc.pivotable.webflux.api.GreetingHandler;
import eu.solven.pepper.unittest.ILogDisabler;
import eu.solven.pepper.unittest.PepperTestHelper;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PivotableContestServerApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_INMEMORY })
@Slf4j
public class TestKumiteApiRouter {

	String v1 = "/api/v1";

	@Autowired
	private WebTestClient webTestClient;

	@Autowired
	PivotableTokenService tokenService;

	protected String generateAccessToken() {
		return tokenService.generateAccessToken(FakeUser.ACCOUNT_ID, Duration.ofMinutes(1), false);
	}

	@Test
	public void testHello() {
		log.debug("About {}", GreetingHandler.class);

		webTestClient

				.get()
				.uri(v1 + "/hello")
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + generateAccessToken())
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(Greeting.class)
				.value(greeting -> {
					Assertions.assertThat(greeting.getMessage()).isEqualTo("Hello, Spring!");
				});
	}

	@Test
	public void testSearchGames() {
		log.debug("About {}", EntrypointsHandler.class);

		webTestClient

				.get()
				.uri(v1 + "/games")
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + generateAccessToken())
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBodyList(AdhocEntrypointMetadata.class)
				.value(games -> {
					Assertions.assertThat(games)
							.hasSizeGreaterThanOrEqualTo(1)

							.anySatisfy(game -> {
								Assertions.assertThat(game.getUrl()).startsWith("http://localhost");
							});
				});
	}

	@Test
	public void testSearchGames_gameId_undefined() {
		log.debug("About {}", EntrypointsHandler.class);

		try (ILogDisabler logDisabler = PepperTestHelper.disableLog(PivotableWebExceptionHandler.class)) {
			webTestClient.get()

					.uri(v1 + "/games?game_id=undefined")
					.accept(MediaType.APPLICATION_JSON)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + generateAccessToken())
					.exchange()

					.expectStatus()
					.isBadRequest();
		}
	}

	@Test
	public void testSearchGames_gameId_tsp() {
		log.debug("About {}", EntrypointsHandler.class);

		webTestClient.get()

				.uri(v1 + "/games?game_id=" + AdhocEntrypointMetadata.localhost().getId())
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + generateAccessToken())
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk();
	}
}