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
package eu.solven.adhoc.pivotable.webnone.api;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;

import eu.solven.adhoc.app.IPivotableSpringProfiles;

public class TestPivotableLoginWebnoneController {
	final ClientRegistration someClientRegistration = ClientRegistration.withRegistrationId("someRegistrationId")
			.authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
			.clientId("someClientId")
			.tokenUri("someTokenUri")
			.build();

	final InMemoryReactiveClientRegistrationRepository clientRegistrationRepository =
			new InMemoryReactiveClientRegistrationRepository(someClientRegistration);

	final MockEnvironment env = new MockEnvironment();;

	final PivotableLoginWebnoneController controller;

	{
		controller = newControllerWith(env, clientRegistrationRepository);
	}

	/**
	 * Build a controller wired to a custom environment + registration repo. Used by the OAuth2-toggle precedence tests,
	 * which need a {@code AUTHORIZATION_CODE}-grant registration (so the controller actually emits a provider entry)
	 * plus a fresh environment for the property toggles.
	 *
	 * @param customEnv
	 *            the environment seen by the controller's `env.getProperty(...)` calls
	 * @param repo
	 *            the reactive registration repo (we mock both servlet + reactive lookups)
	 * @return a controller wired to {@code customEnv} and {@code repo}
	 */
	private static PivotableLoginWebnoneController newControllerWith(MockEnvironment customEnv,
			InMemoryReactiveClientRegistrationRepository repo) {
		ApplicationContext appContext = Mockito.mock(ApplicationContext.class);

		// webflux
		Mockito.when(appContext.getEnvironment()).thenReturn(customEnv);
		Mockito.doReturn(repo).when(appContext).getBean(InMemoryReactiveClientRegistrationRepository.class);

		// webmvc
		ObjectProvider<InMemoryClientRegistrationRepository> mvcBeanProvider = Mockito.mock(ObjectProvider.class);
		Mockito.doReturn(mvcBeanProvider).when(appContext).getBeanProvider(InMemoryClientRegistrationRepository.class);

		return new PivotableLoginWebnoneController(appContext);
	}

	/** Build a typical AUTHORIZATION_CODE-grant registration matching the github/google shape. */
	private static ClientRegistration authCodeRegistration(String registrationId) {
		return ClientRegistration.withRegistrationId(registrationId)
				.authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
				.clientId(registrationId + "-client-id")
				.clientSecret(registrationId + "-client-secret")
				.authorizationUri("https://example.invalid/oauth2/authorize")
				.tokenUri("https://example.invalid/oauth2/token")
				.redirectUri("{baseUrl}/login/oauth2/code/{registrationId}")
				.build();
	}

	@Test
	void testLoginProviders_default() {
		Assertions.assertThat(controller.loginProviders()).containsKeys("list", "map");
		Assertions.assertThat(controller.loginProviders().get("list"))
				.asInstanceOf(InstanceOfAssertFactories.COLLECTION)
				.isEmpty();
		Assertions.assertThat(controller.loginProviders().get("map"))
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.isEmpty();
	}

	@Test
	void testLoginProviders() {
		env.addActiveProfile(IPivotableSpringProfiles.P_FAKEUSER);

		Assertions.assertThat(controller.loginProviders()).containsKeys("list", "map");
		Assertions.assertThat(controller.loginProviders().get("list"))
				.asInstanceOf(InstanceOfAssertFactories.COLLECTION)
				.anySatisfy(lp -> {
					Assertions.assertThat(lp)
							.asInstanceOf(InstanceOfAssertFactories.MAP)
							.containsEntry("type", "basic");
				})
				.hasSize(1);
		Assertions.assertThat(controller.loginProviders().get("map"))
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.hasSize(1);
	}

	/**
	 * An {@code AUTHORIZATION_CODE}-grant registration shows up in the login providers by default — the Java fallback
	 * in {@link PivotableLoginWebnoneController#loginProviders()} is
	 * {@code env.getProperty("adhoc.pivotable.login.oauth2.enabled", Boolean.class, true)}.
	 */
	@Test
	void testLoginProviders_oauth2DefaultTrue_registrationVisible() {
		InMemoryReactiveClientRegistrationRepository repo =
				new InMemoryReactiveClientRegistrationRepository(authCodeRegistration("github"));
		MockEnvironment freshEnv = new MockEnvironment();
		PivotableLoginWebnoneController c = newControllerWith(freshEnv, repo);

		Assertions.assertThat(c.loginProviders().get("map"))
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.containsKey("github");
	}

	/**
	 * Regression: a downstream user's {@code application.yml: adhoc.pivotable.login.oauth2.enabled:
	 * false} MUST disable the buttons even when an AUTHORIZATION_CODE-grant registration is wired (e.g. by the
	 * `pivotable-demo_external_oauth2` profile). Previously broken when the demo profile yaml carried `enabled: true`
	 * itself, since profile yaml beats base yaml — the override was silently masked. After dropping the redundant yaml
	 * lines, the only thing the user has to beat is the Java default {@code true}, which a base-yaml `false` does.
	 */
	@Test
	void testLoginProviders_oauth2DisabledViaProperty_hidesRegistrations() {
		InMemoryReactiveClientRegistrationRepository repo =
				new InMemoryReactiveClientRegistrationRepository(authCodeRegistration("github"));
		MockEnvironment freshEnv = new MockEnvironment();
		freshEnv.setProperty(IPivotableLoginConstants.P_OAUTH2, "false");
		PivotableLoginWebnoneController c = newControllerWith(freshEnv, repo);

		Assertions.assertThat(c.loginProviders().get("map")).asInstanceOf(InstanceOfAssertFactories.MAP).isEmpty();
	}

	/**
	 * Per-provider gate: {@code adhoc.pivotable.login.oauth2.<id>.enabled=false} suppresses that specific registration
	 * while leaving the others visible. Used by deployments that want one provider but not another.
	 */
	@Test
	void testLoginProviders_oauth2PerProviderDisabled_dropsOnlyThatProvider() {
		InMemoryReactiveClientRegistrationRepository repo =
				new InMemoryReactiveClientRegistrationRepository(authCodeRegistration("github"),
						authCodeRegistration("google"));
		MockEnvironment freshEnv = new MockEnvironment();
		freshEnv.setProperty("adhoc.pivotable.login.oauth2.github.enabled", "false");
		PivotableLoginWebnoneController c = newControllerWith(freshEnv, repo);

		Assertions.assertThat(c.loginProviders().get("map"))
				.asInstanceOf(InstanceOfAssertFactories.MAP)
				.doesNotContainKey("github")
				.containsKey("google");
	}
}
