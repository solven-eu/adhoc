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
package eu.solven.adhoc.pivotable.webflux.security.oauth2;

import org.springframework.security.oauth2.client.userinfo.DefaultReactiveOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;

import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.pivotable.account.login.IPivotableTestConstants;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.webnone.security.oauth2.IOnPivotableUser;
import eu.solven.adhoc.pivotable.webnone.security.oauth2.PivotableOAuth2UserWebnoneService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * This is responsible for loading {@link OAuth2User} from an {@link OAuth2UserRequest}, typically through the
 * `/userinfo` endpoint.
 * 
 * We takre advantage of this step to register in our cache the details of going through users, hence automatically
 * registering logging-in Users..
 * 
 * @author Benoit Lacelle
 *
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PivotableReactiveOAuth2UserService extends DefaultReactiveOAuth2UserService
		implements IPivotableOAuth2Constants, IOnPivotableUser {

	@Deprecated
	public static final String PROVIDERID_TEST = IPivotableTestConstants.PROVIDERID_TEST;

	// private final AccountsStore accountsStore;
	final PivotableUsersRegistry usersRegistry;

	@Override
	@SneakyThrows(OAuth2AuthenticationException.class)
	public Mono<OAuth2User> loadUser(OAuth2UserRequest oAuth2UserRequest) {
		log.trace("Load user {}", oAuth2UserRequest);
		Mono<OAuth2User> userFromProvider = super.loadUser(oAuth2UserRequest);
		return processOAuth2User(oAuth2UserRequest, userFromProvider);
	}

	@VisibleForTesting
	public PivotableUser onAdhocUserRaw(PivotableUserPreRegister userPreRegister) {
		return usersRegistry.registerOrUpdate(userPreRegister);
	}

	private Mono<OAuth2User> processOAuth2User(OAuth2UserRequest oAuth2UserRequest,
			Mono<OAuth2User> userFromProviderMono) {
		return userFromProviderMono.map(userFromProvider -> PivotableOAuth2UserWebnoneService
				.processOAuth2User(this, oAuth2UserRequest, userFromProvider));

	}

}