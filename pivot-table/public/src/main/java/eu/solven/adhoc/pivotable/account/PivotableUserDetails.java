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

import java.net.URI;

import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Like {@link PivotableUser} but without knowledge of the accountId.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class PivotableUserDetails {
	// @NonNull
	// KumiteUserRawRaw rawRaw;

	@NonNull
	String username;

	String name;

	String email;

	URI picture;

	// https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
	String countryCode;

	String school;
	String company;

	private PivotableUserDetailsBuilder preloadBuilder() {
		return PivotableUserDetails.builder()
				// .rawRaw(rawRaw)
				.username(username)
				.name(name)
				.email(email)
				.picture(picture)
				.countryCode(countryCode)
				.school(school)
				.company(company);
	}

	public PivotableUserDetails setCountryCode(String countryCode) {
		return preloadBuilder().countryCode(countryCode).build();
	}

	public PivotableUserDetails setCompany(String company) {
		return preloadBuilder().company(company).build();
	}

	public PivotableUserDetails setSchool(String school) {
		return preloadBuilder().school(school).build();
	}
}
