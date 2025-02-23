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
package eu.solven.adhoc.pivottable;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.pivotable.account.InMemoryUserRepository;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.account.fake_user.RandomUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.login.IPivotableTestConstants;
import eu.solven.adhoc.tools.JdkUuidGenerator;

public class TestInMemoryUserRepository {
	InMemoryUserRepository userRepository = new InMemoryUserRepository(JdkUuidGenerator.INSTANCE);

	@Test
	public void testRegisterUser() {
		PivotableUser user = userRepository.registerOrUpdate(IPivotableTestConstants.userPreRegister());

		Optional<PivotableUserRawRaw> optRawRaw = userRepository.getUser(user.getAccountId());
		Assertions.assertThat(optRawRaw).isPresent().contains(user.getRawRaw());
	}

	@Test
	public void testFakeUser() {
		// Not present by default
		{
			Optional<PivotableUserRawRaw> optRawRaw = userRepository.getUser(FakeUser.ACCOUNT_ID);
			Assertions.assertThat(optRawRaw).isEmpty();
		}

		PivotableUser user = userRepository.registerOrUpdate(FakeUser.pre());
		Assertions.assertThat(user.getAccountId()).isEqualTo(FakeUser.ACCOUNT_ID);

		Optional<PivotableUserRawRaw> optRawRaw = userRepository.getUser(user.getAccountId());
		Assertions.assertThat(optRawRaw).isPresent().contains(user.getRawRaw());
	}

	@Test
	public void testRandomUser() {
		// Not present by default
		{
			Optional<PivotableUserRawRaw> optRawRaw = userRepository.getUser(RandomUser.ACCOUNT_ID);
			Assertions.assertThat(optRawRaw).isEmpty();
		}

		PivotableUser user = userRepository.registerOrUpdate(RandomUser.pre());
		Assertions.assertThat(user.getAccountId()).isEqualTo(RandomUser.ACCOUNT_ID);

		Optional<PivotableUserRawRaw> optRawRaw = userRepository.getUser(user.getAccountId());
		Assertions.assertThat(optRawRaw).isPresent().contains(user.getRawRaw());
	}
}
