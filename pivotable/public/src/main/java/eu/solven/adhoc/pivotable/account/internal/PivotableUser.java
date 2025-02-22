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
package eu.solven.adhoc.pivotable.account.internal;

import java.util.UUID;

import eu.solven.adhoc.pivotable.account.PivotableUserDetails;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * User details, typically from an oauth2 provider
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
// This is not public, but used for persistence, IKumiteTestConstants and `player` module
public class PivotableUser {
	// Multiple rawRaw may be attached to the same account (e.g. by using different OAuth2 providers)
	@NonNull
	UUID accountId;

	@NonNull
	PivotableUserRawRaw rawRaw;

	@NonNull
	PivotableUserDetails details;

	@Default
	boolean enabled = true;

	public PivotableUser editDetails(PivotableUserDetails details) {
		return PivotableUser.builder().accountId(accountId).rawRaw(rawRaw).details(details).enabled(enabled).build();
	}

	public static PivotableUserRaw raw(PivotableUser user) {
		return PivotableUserRaw.builder().accountId(user.accountId).details(user.details).enabled(user.enabled).build();
	}

	public static PivotableUserPreRegister pre(PivotableUser user) {
		return PivotableUserPreRegister.builder().rawRaw(user.getRawRaw()).details(user.getDetails()).build();
	}

}
