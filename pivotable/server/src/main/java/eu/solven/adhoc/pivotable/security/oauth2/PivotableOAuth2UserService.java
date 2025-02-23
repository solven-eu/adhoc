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
import java.net.URISyntaxException;
import java.net.URL;

import org.springframework.security.core.authority.SimpleGrantedAuthority;
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
public class PivotableOAuth2UserService extends DefaultReactiveOAuth2UserService {
	public static final String PROVIDERID_GITHUB = "github";
	public static final String PROVIDERID_GOOGLE = "google";

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
		PivotableUser user = usersRegistry.registerOrUpdate(userPreRegister);
		return user;
	}

	private Mono<OAuth2User> processOAuth2User(OAuth2UserRequest oAuth2UserRequest,
			Mono<OAuth2User> userFromProviderMono) {
		return userFromProviderMono.map(userFromProvider -> {
			// The following comment can be used to register new unittest on new registration
			// new ObjectMapper().writeValueAsString(userFromProvider.getAttributes());

			PivotableUserRawRaw rawRaw = oauth2ToRawRaw(userFromProvider);

			String registrationId = oAuth2UserRequest.getClientRegistration().getRegistrationId();
			if (!registrationId.equals(rawRaw.getProviderId())) {
				throw new IllegalStateException("Inconsistent providerId inference from OAuth2User");
			}

			String keyForPicture = switch (registrationId) {
			case PROVIDERID_GITHUB:
				yield "avatar_url";
			case PROVIDERID_GOOGLE:
				yield "picture";
			default:
				throw new IllegalArgumentException("Unexpected value: " + registrationId);
			};
			PivotableUserDetails.PivotableUserDetailsBuilder kumiteUserBuilder = PivotableUserDetails.builder()
					// .rawRaw(rawRaw)
					.username(userFromProvider.getAttributes().get("name").toString())
					.name(userFromProvider.getAttributes().get("name").toString())
					.email(userFromProvider.getAttributes().get("email").toString());

			Object rawPicture = userFromProvider.getAttributes().get(keyForPicture);
			if (rawPicture != null) {
				kumiteUserBuilder = kumiteUserBuilder.picture(URI.create(rawPicture.toString()));
			}
			PivotableUserDetails rawUser = kumiteUserBuilder.build();

			PivotableUserPreRegister userPreRegister =
					PivotableUserPreRegister.builder().rawRaw(rawRaw).details(rawUser).build();

			PivotableUser user = onAdhocUserRaw(userPreRegister);

			log.trace("User info is {}", user);
			return userFromProvider;
		});

	}

	public static PivotableUserRawRaw oauth2ToRawRaw(OAuth2User o) {
		String providerId = guessProviderId(o);
		String sub = getSub(providerId, o);
		PivotableUserRawRaw rawRaw = PivotableUserRawRaw.builder().providerId(providerId).sub(sub).build();
		return rawRaw;
	}

	private static String getSub(String providerId, OAuth2User o) {
		if (PROVIDERID_GITHUB.equals(providerId)) {
			Object id = o.getAttribute("id");
			if (id == null) {
				throw new IllegalStateException("Invalid id: " + id);
			}
			return id.toString();
		} else if (PROVIDERID_GOOGLE.equals(providerId)) {
			Object sub = o.getAttribute("sub");
			if (sub == null) {
				throw new IllegalStateException("Invalid sub: " + sub);
			}
			return sub.toString();
		} else if (PROVIDERID_TEST.equals(providerId)) {
			Object id = o.getAttribute("id");
			if (id == null) {
				throw new IllegalStateException("Invalid sub: " + id);
			}
			return id.toString();
		} else {
			throw new IllegalStateException("Not managed providerId: " + providerId);
		}
	}

	private static String guessProviderId(OAuth2User o) {
		if (isGoogle(o)) {
			return PROVIDERID_GOOGLE;
		} else if (PROVIDERID_TEST.equals(o.getAttribute("providerId"))) {
			return PROVIDERID_TEST;
		}
		return PROVIDERID_GITHUB;
	}

	private static boolean isGoogle(OAuth2User o) {
		if (o.getAuthorities()
				.contains(new SimpleGrantedAuthority("SCOPE_https://www.googleapis.com/auth/userinfo.email"))) {
			return true;
		}

		// TODO Unclear when we receive iss or not (Changed around introducing
		// SocialWebFluxSecurity.oidcReactiveOAuth2UserService)
		URL issuer = (URL) o.getAttribute("iss");
		if (issuer == null) {
			return false;
		}
		URI uri;
		try {
			uri = issuer.toURI();
		} catch (URISyntaxException e) {
			throw new IllegalStateException("Invalid iss: `%s`".formatted(issuer), e);
		}
		return "https://accounts.google.com".equals(uri.toASCIIString());
	}

}