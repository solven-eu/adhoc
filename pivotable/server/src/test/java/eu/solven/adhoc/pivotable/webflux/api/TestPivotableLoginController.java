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
package eu.solven.adhoc.pivotable.webflux.api;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.InMemoryUserRepository;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.tools.IUuidGenerator;
import eu.solven.adhoc.tools.JdkUuidGenerator;

public class TestPivotableLoginController {
	final ClientRegistration someClientRegistration = ClientRegistration.withRegistrationId("someRegistrationId")
			.authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
			.clientId("someClientId")
			.tokenUri("someTokenUri")
			.build();

	final InMemoryReactiveClientRegistrationRepository clientRegistrationRepository =
			new InMemoryReactiveClientRegistrationRepository(someClientRegistration);

	final IUuidGenerator uuidGenerator = new JdkUuidGenerator();

	final InMemoryUserRepository userRepository = new InMemoryUserRepository(uuidGenerator);

	final PivotableUsersRegistry usersRegistry = new PivotableUsersRegistry(userRepository, userRepository);
	final MockEnvironment env = new MockEnvironment() {
		{
			setProperty(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER, "https://unit.test.adhoc");
			setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY,
					PivotableTokenService.generateSignatureSecret(JdkUuidGenerator.INSTANCE).toJSONString());
		}
	};

	final PivotableTokenService kumiteTokenService = new PivotableTokenService(env, uuidGenerator);

	final PivotableLoginController controller =
			new PivotableLoginController(clientRegistrationRepository, usersRegistry, env, kumiteTokenService);

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
}
