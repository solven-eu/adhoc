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
package eu.solven.adhoc.pivotable.app;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.account.fake_user.RandomUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import lombok.extern.slf4j.Slf4j;

/**
 * Register a {@link FakeUser}, for simpler authentication scenarios.
 * 
 * @author Benoit Lacelle
 */
@Configuration
@Slf4j
public class InjectPivotableAccountsConfig {

	// `java:S6831` as Sonar states `@Qualifier` is bad on `@Bean`
	@SuppressWarnings("java:S6831")
	@Profile(IPivotableSpringProfiles.P_FAKEUSER)
	@Qualifier(IPivotableSpringProfiles.P_FAKEUSER)
	@Bean
	public PivotableUser initFakeUser(PivotableUsersRegistry usersRegistry) {
		log.info("Registering the {} users", IPivotableSpringProfiles.P_FAKEUSER);

		return usersRegistry.registerOrUpdate(FakeUser.pre());
	}

	// `java:S6831` as Sonar states `@Qualifier` is bad on `@Bean`
	@Qualifier("random")
	@Bean
	public PivotableUser initRandomUser(PivotableUsersRegistry usersRegistry) {
		log.info("Registering the random user");

		return usersRegistry.registerOrUpdate(RandomUser.pre());
	}
}
