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
package eu.solven.adhoc.pivotable.webflux.app.it.security;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseCookie;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.FluxExchangeResult;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.app.PivotableJackson;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginWebfluxController;
import lombok.extern.slf4j.Slf4j;

/**
 * Re-check {@link TestSecurity_WithOAuth2_asOAuth2User}, with the configuration activating a fake BASIC user.
 * 
 * @author Benoit Lacelle
 *
 */
@ExtendWith(SpringExtension.class)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_FAKEUSER })
@Slf4j
public class TestSecurity_WithOAuth2_asFakeBasicUser extends TestSecurity_WithOAuth2_asOAuth2User {

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void onLoginOptions(Map loginOptions) {
		Map<String, ?> asMap = (Map<String, ?>) loginOptions.get("map");
		Assertions.assertThat(asMap).hasSize(3).containsKeys("github", "google", IPivotableSpringProfiles.P_FAKEUSER);

		Assertions.assertThat((Map) asMap.get("github")).containsEntry("login_url", "/oauth2/authorization/github");

		List<Map<String, ?>> asList = (List<Map<String, ?>>) loginOptions.get("list");
		Assertions.assertThat(asList).hasSize(3).anySatisfy(m -> {
			Assertions.assertThat((Map) m).containsEntry("login_url", "/oauth2/authorization/github").hasSize(4);
		}).anySatisfy(m -> {
			Assertions.assertThat((Map) m).containsEntry("login_url", "/oauth2/authorization/google");
		}).anySatisfy(m -> {
			Assertions.assertThat((Map) m).containsEntry("login_url", "/html/login/basic");
		});
	}

	@Test
	@Override
	public void testLoginAccessToken() {
		log.debug("About {}", PivotableLoginWebfluxController.class);

		webTestClient

				.get()
				.uri("/api/login/v1/oauth2/token")
				.accept(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION,
						"Basic " + HttpHeaders
								.encodeBasicAuth(FakeUser.ACCOUNT_ID.toString(), "no_password", StandardCharsets.UTF_8))
				.exchange()

				.expectStatus()
				.isOk()
				.expectBody(AccessTokenWrapper.class)
				.value(token -> {
					Map asMap = PivotableJackson.objectMapper().convertValue(token, Map.class);

					Assertions.assertThat(asMap)
							.containsKey("access_token")
							.containsEntry("token_type", "Bearer")
							.containsEntry("expires_in", 3600L)
							.hasSize(3);
				});
	}

	@Test
	public void testLoginAccessToken_invalidUser() {
		log.debug("About {}", PivotableLoginWebfluxController.class);

		webTestClient

				.get()
				.uri("/api/login/v1/oauth2/token")
				.accept(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION,
						"Basic " + HttpHeaders
								.encodeBasicAuth("someUnknownUser", "no_password", StandardCharsets.UTF_8))
				.exchange()

				.expectStatus()
				.isUnauthorized();
	}

	@Test
	public void testLoginBasic() {
		log.debug("About {}", PivotableLoginWebfluxController.class);

		webTestClient

				// https://www.baeldung.com/spring-security-csrf
				.mutateWith(SecurityMockServerConfigurers.csrf())

				.post()
				.uri("/api/login/v1/basic")
				.accept(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION,
						"Basic " + HttpHeaders
								.encodeBasicAuth("someUnknownUser", "no_password", StandardCharsets.UTF_8))
				.exchange()

				.expectStatus()
				.isUnauthorized();
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testLoginBasic_fakeUser() throws Exception {
		log.debug("About {}", PivotableLoginWebfluxController.class);

		// Step 1: authenticate via BASIC and capture the SESSION cookie.
		// In WebFlux, AuthenticationWebFilter explicitly calls securityContextRepository.save() after successful
		// authentication, so the SESSION cookie is set automatically — unlike the servlet BasicAuthenticationFilter.
		// https://docs.spring.io/spring-security/reference/migration/servlet/session-management.html#_require_explicit_saving_of_securitycontextrepository
		FluxExchangeResult<Map> loginResult = webTestClient

				// https://www.baeldung.com/spring-security-csrf
				.mutateWith(SecurityMockServerConfigurers.csrf())

				.post()
				.uri("/api/login/v1/basic")
				.accept(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION,
						"Basic " + HttpHeaders
								.encodeBasicAuth(FakeUser.ACCOUNT_ID.toString(), "no_password", StandardCharsets.UTF_8))
				.exchange()

				.expectStatus()
				.isOk()

				.expectCookie()
				.exists("SESSION")

				.returnResult(Map.class);

		ResponseCookie sessionCookie = loginResult.getResponseCookies().getFirst("SESSION");
		Assertions.assertThat(sessionCookie).isNotNull();

		// Step 2: replay the SESSION cookie — the authenticated SecurityContext should be restored
		webTestClient.get()
				.uri("/api/login/v1/json")
				.accept(MediaType.APPLICATION_JSON)
				.cookie(sessionCookie.getName(), sessionCookie.getValue())
				.exchange()

				.expectStatus()
				.isOk()

				.expectBody(Map.class)
				.value(body -> {
					Assertions.assertThat(body).containsEntry("login", 200);
				});
	}
}