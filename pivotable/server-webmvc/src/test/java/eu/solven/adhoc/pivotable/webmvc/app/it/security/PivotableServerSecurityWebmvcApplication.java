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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.pivotable.account.InMemoryUserRepository;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.app.InjectPivotableAccountsConfig;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.ActiveRefreshTokens;
import eu.solven.adhoc.pivotable.webmvc.PivotableWebmvcExceptionHandler;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableSpaController;
import eu.solven.adhoc.pivotable.webmvc.security.PivotableSecurityWebmvcSpringConfig;
import eu.solven.adhoc.tools.PivotableRandomConfiguration;

@SpringBootApplication(scanBasePackages = "none")
@Import({ PivotableRandomConfiguration.class,

		PivotableSecurityWebmvcSpringConfig.class,

		PivotableUsersRegistry.class,
		InMemoryUserRepository.class,

		PivotableSpaController.class,

		ActiveRefreshTokens.class,

		InjectPivotableAccountsConfig.class,

		PivotableWebmvcExceptionHandler.class,

})
public class PivotableServerSecurityWebmvcApplication {

	public static void main(String[] args) {
		SpringApplication.run(PivotableServerSecurityWebmvcApplication.class, args);
	}

}
