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

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.session.ReactiveMapSessionRepository;
import org.springframework.session.ReactiveSessionRepository;

import eu.solven.adhoc.pivotable.account.AdhocUsersRegistry;
import eu.solven.adhoc.pivotable.account.InMemoryUserRepository;
import eu.solven.adhoc.pivotable.app.InjectKumiteAccountsConfig;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.ActiveRefreshTokens;
import eu.solven.adhoc.pivotable.security.PivotableSecuritySpringConfig;
import eu.solven.adhoc.pivotable.security.tokens.AccessTokenHandler;
import eu.solven.adhoc.pivotable.webflux.api.AdhocSpaRouter;
import eu.solven.adhoc.pivotable.webflux.api.GreetingHandler;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginRouter;
import eu.solven.adhoc.tools.AdhocRandomConfiguration;

@SpringBootApplication(scanBasePackages = "none")
@Import({ AdhocRandomConfiguration.class,

		PivotableSecuritySpringConfig.class,

		AdhocUsersRegistry.class,
		InMemoryUserRepository.class,

		AdhocSpaRouter.class,
		GreetingHandler.class,

		PivotableLoginRouter.class,
		// PlayerVerifierFilterFunction.class,
		AccessTokenHandler.class,
		ActiveRefreshTokens.class,

		InjectKumiteAccountsConfig.class,

})
public class KumiteServerSecurityApplication {

	public static void main(String[] args) {
		SpringApplication.run(KumiteServerSecurityApplication.class, args);
	}

	// https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-3.0-Migration-Guide#spring-session-store-type
	@Bean
	public ReactiveSessionRepository<?> inmemorySessionRepository() {
		return new ReactiveMapSessionRepository(new ConcurrentHashMap<>());
	}

}
