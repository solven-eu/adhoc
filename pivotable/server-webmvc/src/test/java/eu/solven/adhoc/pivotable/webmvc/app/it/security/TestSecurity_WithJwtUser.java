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
package eu.solven.adhoc.pivotable.webmvc.app.it.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.StatusAssertions;
import org.springframework.test.web.reactive.server.WebTestClient;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.RandomUser;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.webmvc.PivotableWebmvcExceptionHandler;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableLoginWebmvcController;
import eu.solven.adhoc.pivotable.webmvc.security.tokens.AccessTokenController;
import eu.solven.adhoc.pivotable.webnone.api.GreetingController;
import eu.solven.pepper.unittest.ILogDisabler;
import eu.solven.pepper.unittest.PepperTestHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * JWT enables logging-in a subset of APIs, especially the applicative (e.g. not login) APIs.
 *
 * @author Benoit Lacelle
 *
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PivotableServerSecurityWebmvcApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.MOCK,
		properties = { IPivotableSpringProfiles.P_CONFIG_IMPORT// , "logging.level.org.springframework.security=DEBUG"
		})
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE })
@Slf4j
// TODO There is weird interactions between MockMvc and WebTestClient
@AutoConfigureMockMvc
// https://stackoverflow.com/questions/73881370/mocking-oauth2-client-with-webtestclient-for-servlet-applications-results-in-nul
@AutoConfigureWebTestClient
public class TestSecurity_WithJwtUser {

	@Autowired
	WebTestClient webTestClient;

	@Autowired
	PivotableTokenService tokenService;

	protected String generateAccessToken() {
		return tokenService.generateAccessToken(RandomUser.ACCOUNT_ID, Duration.ofMinutes(1), false);
	}

	protected String generateRefreshToken() {
		return tokenService.generateAccessToken(RandomUser.ACCOUNT_ID, Duration.ofMinutes(1), true);
	}

	protected WebTestClient loggedInClient() {
		return webTestClient.mutateWith((b, h, c) -> {
			b.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + generateAccessToken());
		});
	}

	@Test
	public void testApiPublic() {
		log.debug("About {}", GreetingController.class);

		loggedInClient()

				.get()
				.uri("/api/v1/public")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(String.class)
				.value(greeting -> assertThat(greeting).isEqualTo("This is a public endpoint"));
	}

	@Test
	public void testLogin() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		loggedInClient()

				.get()
				.uri("/html/login")
				.accept(MediaType.TEXT_HTML)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(String.class)
				.value(greeting -> assertThat(greeting).contains("<title>Pivotable"));
	}

	@Test
	public void testLoginOptions() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		loggedInClient()

				.get()
				.uri("/api/login/v1/providers")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(Map.class);
	}

	@Test
	public void testLoginUser() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		try (ILogDisabler logDisabler = PepperTestHelper.disableLog(PivotableWebmvcExceptionHandler.class)) {
			loggedInClient()

					.get()
					.uri("/api/login/v1/user")
					.accept(MediaType.APPLICATION_JSON)
					.exchange()

					.expectStatus()
					// This routes requires OAuth2 authentication
					.isUnauthorized();
		}
	}

	@Test
	public void testLoginToken() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		try (ILogDisabler logDisabler = PepperTestHelper.disableLog(PivotableWebmvcExceptionHandler.class)) {
			loggedInClient()

					.get()
					.uri("/api/login/v1/oauth2/token")
					.accept(MediaType.APPLICATION_JSON)
					.exchange()

					.expectStatus()
					// This routes requires OAuth2 authentication
					.isUnauthorized();
		}
	}

	@Test
	public void testLoginPage() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		loggedInClient()

				.get()
				.uri("/api/login/v1/html")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				// This routes requires OAuth2 authentication
				.isFound()
				.expectHeader()
				.location("login");
	}

	@Test
	public void testApiPrivate() {
		log.debug("About {}", GreetingController.class);

		loggedInClient()

				.get()
				.uri("/api/v1/private")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk();
	}

	@Test
	public void testApiPrivate_unknownRoute() {
		log.debug("About {}", GreetingController.class);

		loggedInClient()

				.get()
				.uri("/api/private/unknown")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isNotFound();
	}

	@Test
	public void testApiPOSTWithCsrf() {
		log.debug("About {}", GreetingController.class);

		loggedInClient().post()
				.uri("/api/v1/hello")
				.bodyValue("{}")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk();
	}

	@Test
	public void testApiPOSTWithoutCsrf() {
		log.debug("About {}", GreetingController.class);

		loggedInClient().post()
				.uri("/api/v1/hello")
				.bodyValue("{}")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk();
	}

	@Test
	public void testMakeRefreshToken() {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		try (ILogDisabler logDisabler = PepperTestHelper.disableLog(PivotableWebmvcExceptionHandler.class)) {
			StatusAssertions expectStatus = loggedInClient()

					.get()
					.uri("/api/login/v1/oauth2/token?refresh_token=true")
					.accept(MediaType.APPLICATION_JSON)
					.exchange()
					.expectStatus();

			// We need an oauth2 user, not a jwt user
			expectStatus.isUnauthorized()
					.expectBody(byte[].class)
					// .expectBody(Map.class)
					.value(bodyAsMap -> {
						Assertions.assertThat(bodyAsMap)
								// .containsEntry("error_message", "No user").hasSize(1)
								.contains();
					});

		}
	}

	@Test
	public void testRefreshTokenToAccessToken() {
		log.debug("About {}", AccessTokenController.class);

		loggedInClient()

				.get()
				.uri("/api/v1/oauth2/token")
				// FORCE A REFRESH_TOKEN AS HEADER
				.header(HttpHeaders.AUTHORIZATION, "Bearer " + generateRefreshToken())
				.accept(MediaType.APPLICATION_JSON)
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(AccessTokenWrapper.class)
				.value(accessTokenHolder -> {
					Assertions.assertThat(accessTokenHolder.getAccessToken()).isNotEmpty();
				});
	}
}
