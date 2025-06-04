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
package eu.solven.adhoc.pivotable.account.fake_user;

import java.util.UUID;

import eu.solven.adhoc.pivotable.account.PivotableUserDetails;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import lombok.experimental.UtilityClass;

/**
 * The fake-user is useful when we want to have a trivial authentication, for unit-tests or integrations.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class FakeUser {

	// IKumiteSpringProfiles.P_DEFAULT_FAKE_USER
	public static final UUID ACCOUNT_ID = UUID.fromString("11111111-1111-1111-1111-000000000000");

	public static PivotableUserRawRaw rawRaw() {
		return PivotableUserRawRaw.builder().providerId("fakeProviderId").sub("fakeSub").build();
	}

	public static PivotableUserPreRegister pre() {
		PivotableUserDetails details =
				PivotableUserDetails.builder().username("fakeUsername").email("fake@fake").name("Fake User").build();
		return PivotableUserPreRegister.builder().rawRaw(rawRaw()).details(details).build();
	}

	public static PivotableUser user() {
		PivotableUserPreRegister pre = pre();
		return PivotableUser.builder().accountId(ACCOUNT_ID).rawRaw(pre.getRawRaw()).details(pre.getDetails()).build();
	}

}
