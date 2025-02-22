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
package eu.solven.adhoc.pivotable.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.JwtUserContextHolder;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableJwtWebFluxSecurity;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.pivotable.webflux.PivotableWebExceptionHandler;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginController;
import eu.solven.adhoc.pivotable.webflux.api.PivotableMetadataController;
import eu.solven.adhoc.pivotable.webflux.api.PivotablePublicController;
import lombok.extern.slf4j.Slf4j;

// https://docs.spring.io/spring-security/reference/reactive/oauth2/login/advanced.html#webflux-oauth2-login-advanced-userinfo-endpoint
@EnableWebFluxSecurity
@Import({

		PivotableSocialWebFluxSecurity.class,
		PivotableJwtWebFluxSecurity.class,

		PivotablePublicController.class,
		PivotableLoginController.class,
		PivotableMetadataController.class,

		PivotableResourceServerConfiguration.class,
		PivotableTokenService.class,

		JwtUserContextHolder.class,

		PivotableWebExceptionHandler.class,

})
@Slf4j
public class PivotableSecuritySpringConfig {

	@Profile(IPivotableSpringProfiles.P_PRDMODE)
	@Bean
	public Void checkSecured(Environment env) {
		boolean acceptUnsafe = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_UNSAFE,
				IPivotableSpringProfiles.P_UNSAFE_SERVER,
				IPivotableSpringProfiles.P_FAKEUSER,
				IPivotableSpringProfiles.P_UNSAFE_EXTERNAL_OAUTH2));

		if (acceptUnsafe) {
			throw new IllegalStateException("At least one unsafe profile is activated");
		}

		if ("NEEDS_TO_BE_DEFINED".equals(env.getProperty("kumite.login.oauth2.github.clientId"))) {
			log.warn("We lack a proper environment variable for XXX");
		} else if ("NEEDS_TO_BE_DEFINED".equals(env.getProperty("kumite.login.oauth2.github.clientSecret"))) {
			log.warn("We lack a proper environment variable for XXX");
		}

		return null;
	}

	@Bean
	public Void checkSpringProfilesConsistency(Environment env) {
		boolean acceptInMemory = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_INMEMORY));
		boolean acceptRedis = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_REDIS));

		if (acceptRedis && acceptInMemory) {
			throw new IllegalStateException("Can not be both " + IPivotableSpringProfiles.P_INMEMORY
					+ " and "
					+ IPivotableSpringProfiles.P_REDIS);
		}

		return null;
	}

	// We print a refreshToken at startup, as it makes it easier to configure a player
	// @Bean
	// public Void printRandomPlayerRefreshToken(@Qualifier("random") AdhocUser user, PivotableTokenService
	// tokenService) {
	// UUID accountId = user.getAccountId();
	//
	// RefreshTokenWrapper refreshToken = tokenService.wrapInJwtRefreshToken(accountId);
	//
	// log.info("refresh_token for accountId={}: {}", user.getAccountId(), refreshToken);
	//
	// return null;
	// }
}