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

import java.util.Optional;
import java.util.UUID;

import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import lombok.RequiredArgsConstructor;

/**
 * Manages users.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public final class PivotableUsersRegistry {

	// This is a cache of the external information about a user
	// This is useful to enrich some data about other players (e.g. a Leaderboard)
	final IAdhocUserRepository userRepository;

	// We may have multiple users for a single account
	// This maps to the latest/main one
	final IAdhocUserRawRawRepository userRawRawRepository;

	public Optional<PivotableUser> optUser(UUID accountId) {
		return userRawRawRepository.getUser(accountId).map(this::getUser);
	}

	public PivotableUser getUser(UUID accountId) {
		return optUser(accountId).orElseThrow(() -> new IllegalArgumentException("No accountId=" + accountId));
	}

	public PivotableUser getUser(PivotableUserRawRaw rawUser) {
		return userRepository.getUser(rawUser).orElseThrow(() -> new IllegalArgumentException("No rawUser=" + rawUser));
	}

	/**
	 * 
	 * @param userPreRegister
	 * @return a {@link PivotableUser}. This may be a new account if this was not known. If this was already known, we
	 *         update the oauth2 details and return an existing accountId
	 */
	public PivotableUser registerOrUpdate(PivotableUserPreRegister userPreRegister) {
		return userRepository.registerOrUpdate(userPreRegister);
	}
}
