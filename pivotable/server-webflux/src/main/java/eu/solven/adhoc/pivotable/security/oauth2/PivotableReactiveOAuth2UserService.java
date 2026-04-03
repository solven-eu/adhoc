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
package eu.solven.adhoc.pivotable.security.oauth2;

import java.net.URI;

import org.springframework.security.oauth2.client.userinfo.DefaultReactiveOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;

import eu.solven.adhoc.pivotable.account.PivotableUserDetails;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.pivotable.account.login.IPivotableTestConstants;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
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
		implements IPivotableOAuth2Constants {

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
		return userFromProviderMono.map(userFromProvider -> {
			// The following comment can be used to register new unittest on new registration
			// new ObjectMapper().writeValueAsString(userFromProvider.getAttributes());

			PivotableUserRawRaw rawRaw = PivotableOAuth2UserWebnoneService.oauth2ToRawRaw(userFromProvider);

			String registrationId = oAuth2UserRequest.getClientRegistration().getRegistrationId();
			if (!registrationId.equals(rawRaw.getProviderId())) {
				throw new IllegalStateException("Inconsistent providerId inference from OAuth2User");
			}

			String keyForPicture = switch (registrationId) {
			case PivotableOAuth2UserWebnoneService.PROVIDERID_GITHUB:
				yield "avatar_url";
			case PivotableOAuth2UserWebnoneService.PROVIDERID_GOOGLE:
				yield "picture";
			default:
				throw new IllegalArgumentException("Unexpected value: " + registrationId);
			};
			PivotableUserDetails.PivotableUserDetailsBuilder userBuilder = PivotableUserDetails.builder()
					// .rawRaw(rawRaw)
					.username(userFromProvider.getAttributes().get("name").toString())
					.name(userFromProvider.getAttributes().get("name").toString())
					.email(userFromProvider.getAttributes().get("email").toString());

			Object rawPicture = userFromProvider.getAttributes().get(keyForPicture);
			if (rawPicture != null) {
				userBuilder = userBuilder.picture(URI.create(rawPicture.toString()));
			}
			PivotableUserDetails rawUser = userBuilder.build();

			PivotableUserPreRegister userPreRegister =
					PivotableUserPreRegister.builder().rawRaw(rawRaw).details(rawUser).build();

			PivotableUser user = onAdhocUserRaw(userPreRegister);

			log.trace("User info is {}", user);
			return userFromProvider;
		});

	}

}