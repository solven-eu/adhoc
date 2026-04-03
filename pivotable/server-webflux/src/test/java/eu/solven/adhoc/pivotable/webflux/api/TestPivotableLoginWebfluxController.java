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

import java.security.SecureRandom;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;

import eu.solven.adhoc.pivotable.account.InMemoryUserRepository;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.tools.IUuidGenerator;
import eu.solven.adhoc.tools.JdkUuidGenerator;

public class TestPivotableLoginWebfluxController {
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
	final MockEnvironment env = new MockEnvironment();;

	final PivotableTokenService tokenService;

	final PivotableLoginWebfluxController controller;

	{
		env.setProperty(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER, "https://unit.test.adhoc");
		SecureRandom secureRandom = new SecureRandom(new byte[] { 0, 1, 2 });
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY,
				PivotableResourceServerConfiguration.generateSignatureSecret(secureRandom, JdkUuidGenerator.INSTANCE)
						.toJSONString());

		tokenService = new PivotableTokenService(env, uuidGenerator);
		ApplicationContext appContext = Mockito.mock(ApplicationContext.class);
		Mockito.when(appContext.getEnvironment()).thenReturn(new MockEnvironment());
		Mockito.doReturn(clientRegistrationRepository)
				.when(appContext)
				.getBean(InMemoryReactiveClientRegistrationRepository.class);
		controller = new PivotableLoginWebfluxController(appContext, usersRegistry, env, tokenService);
	}

	@Test
	@Disabled("TODO")
	void loginRoute() {
		Assertions.assertThat(controller.user().block().getDetails().getName()).isEqualTo("???");
	}
}
