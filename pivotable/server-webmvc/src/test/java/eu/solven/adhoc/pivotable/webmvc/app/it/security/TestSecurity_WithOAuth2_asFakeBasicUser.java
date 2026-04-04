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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.app.PivotableJackson;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableLoginWebmvcController;
import jakarta.servlet.http.HttpSession;
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
		log.debug("About {}", PivotableLoginWebmvcController.class);

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
		log.debug("About {}", PivotableLoginWebmvcController.class);

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
	public void testLoginBasic_unknownUser() throws Exception {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		// Use MockMvc to inject a valid CSRF token (required by the session-based security chain)
		mockMvc.perform(MockMvcRequestBuilders.post("/api/login/v1/basic")
				.with(SecurityMockMvcRequestPostProcessors.csrf())
				.accept(MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION,
						"Basic " + HttpHeaders
								.encodeBasicAuth("someUnknownUser", "no_password", StandardCharsets.UTF_8)))
				.andExpect(MockMvcResultMatchers.status().isUnauthorized());
	}

	@Test
	public void testLoginBasic_fakeUser() throws Exception {
		log.debug("About {}", PivotableLoginWebmvcController.class);

		// Step 1: authenticate via BASIC and capture the session cookie
		MvcResult loginResult = mockMvc
				.perform(MockMvcRequestBuilders.post("/api/login/v1/basic")
						.with(SecurityMockMvcRequestPostProcessors.csrf())
						.accept(MediaType.APPLICATION_JSON)
						.header(HttpHeaders.AUTHORIZATION,
								"Basic " + HttpHeaders.encodeBasicAuth(FakeUser.ACCOUNT_ID.toString(),
										"no_password",
										StandardCharsets.UTF_8)))
				.andExpect(MockMvcResultMatchers.status().isOk())
				.andReturn();

		// MockMvc does not set Set-Cookie response headers for sessions (the real servlet container does that);
		// carry the session object directly instead.
		HttpSession session = loginResult.getRequest().getSession(false);
		Assertions.assertThat(session).isNotNull();

		// Step 2: replay the session — the authenticated SecurityContext should be restored
		mockMvc.perform(MockMvcRequestBuilders.get("/api/login/v1/json")
				.accept(MediaType.APPLICATION_JSON)
				.session((MockHttpSession) session))
				.andExpect(MockMvcResultMatchers.status().isOk())
				.andExpect(MockMvcResultMatchers.jsonPath("$.login").value(200));
	}
}
