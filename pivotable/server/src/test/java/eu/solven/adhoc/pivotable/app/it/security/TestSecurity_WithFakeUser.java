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
package eu.solven.adhoc.pivotable.app.it.security;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginController;
import eu.solven.adhoc.pivottable.app.PivotableJackson;
import lombok.extern.slf4j.Slf4j;

/**
 * OAuth2 enables logging-in a subset of APIs, especially the login APIs.
 * 
 * @author Benoit Lacelle
 *
 */
@ExtendWith(SpringExtension.class)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_FAKEUSER })
@Slf4j
public class TestSecurity_WithFakeUser extends TestSecurity_WithOAuth2User {

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void onLoginOptions(Map loginOptions) {
		Map<String, ?> asMap = (Map<String, ?>) loginOptions.get("map");
		Assertions.assertThat(asMap).hasSize(3).containsKeys("github", "google", "fakeuser");

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
		log.debug("About {}", PivotableLoginController.class);

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
		log.debug("About {}", PivotableLoginController.class);

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
		log.debug("About {}", PivotableLoginController.class);

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
}