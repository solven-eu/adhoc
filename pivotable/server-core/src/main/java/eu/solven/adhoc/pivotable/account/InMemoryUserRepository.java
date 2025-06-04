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
package eu.solven.adhoc.pivotable.account;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.account.fake_user.RandomUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.tools.IUuidGenerator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Manage users in-memory.
 * 
 * @author Benoit Lacelle
 */
@AllArgsConstructor
@Slf4j
public class InMemoryUserRepository implements IAdhocUserRepository, IAdhocUserRawRawRepository {
	final Map<PivotableUserRawRaw, PivotableUser> accountIdToUser = new ConcurrentHashMap<>();
	final Map<UUID, PivotableUserRawRaw> accountIdToRawRaw = new ConcurrentHashMap<>();

	final IUuidGenerator uuidGenerator;

	@Override
	public Optional<PivotableUser> getUser(PivotableUserRawRaw accountId) {
		return Optional.ofNullable(accountIdToUser.get(accountId));
	}

	@Override
	public Optional<PivotableUserRawRaw> getUser(UUID userRawRaw) {
		return Optional.ofNullable(accountIdToRawRaw.get(userRawRaw));
	}

	@Override
	public void putIfAbsent(UUID accountId, PivotableUserRawRaw rawRaw) {
		accountIdToRawRaw.putIfAbsent(accountId, rawRaw);
	}

	@Override
	public PivotableUser registerOrUpdate(PivotableUserPreRegister userPreRegister) {
		PivotableUserRawRaw rawRaw = userPreRegister.getRawRaw();

		return accountIdToUser.compute(rawRaw, (k, alreadyIn) -> {
			PivotableUser.PivotableUserBuilder userBuilder = PivotableUser.builder()
					.rawRaw(rawRaw)
					// TODO We should merge with pre-existing details
					.details(userPreRegister.getDetails());
			if (alreadyIn == null) {
				UUID accountId = generateAccountId(rawRaw);

				putIfAbsent(accountId, rawRaw);

				userBuilder.accountId(accountId);
			} else {
				userBuilder.accountId(alreadyIn.getAccountId());
			}

			return userBuilder.build();
		});
	}

	protected UUID generateAccountId(PivotableUserRawRaw rawRaw) {
		return generateAccountId(uuidGenerator, rawRaw);
	}

	public static UUID generateAccountId(IUuidGenerator uuidGenerator, PivotableUserRawRaw rawRaw) {
		if (rawRaw.equals(FakeUser.rawRaw())) {
			return FakeUser.ACCOUNT_ID;
		} else if (rawRaw.equals(RandomUser.rawRaw())) {
			return RandomUser.ACCOUNT_ID;
		}
		return uuidGenerator.randomUUID();
	}

}
