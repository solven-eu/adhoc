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
package eu.solven.adhoc.pivotable.webflux.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.server.csrf.CsrfToken;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.PivotableUserDetails;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserRaw;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.security.LoginRouteButNotAuthenticatedException;
import eu.solven.adhoc.pivotable.security.oauth2.PivotableOAuth2UserService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * This {@link Controller} is dedicated to `js` usage. It bridges with the API by providing `refresh_token`s.
 * 
 * @author Benoit Lacelle
 *
 */
@RestController
@RequestMapping("/api/login/v1")
@AllArgsConstructor
@Slf4j
public class PivotableLoginController {

	final InMemoryReactiveClientRegistrationRepository clientRegistrationRepository;

	final PivotableUsersRegistry usersRegistry;
	final Environment env;

	final PivotableTokenService kumiteTokenService;

	@GetMapping("/providers")
	public Map<String, ?> loginProviders() {
		Map<String, Object> registrationIdToDetails = new TreeMap<>();

		StreamSupport.stream(clientRegistrationRepository.spliterator(), false)
				.filter(registration -> AuthorizationGrantType.AUTHORIZATION_CODE
						.equals(registration.getAuthorizationGrantType()))
				.forEach(r -> {
					// Typically 'github' or 'google'
					String registrationId = r.getRegistrationId();
					String loginUrl = "/oauth2/authorization/%s".formatted(registrationId);

					Map<String, String> details = new LinkedHashMap<>();

					details.put("type", "oauth2");
					details.put("registration_id", registrationId);
					details.put("login_url", loginUrl);

					if (PivotableOAuth2UserService.PROVIDERID_GOOGLE.equals(registrationId)) {
						// https://developers.google.com/identity/branding-guidelines?hl=fr
						details.put("button_img", "/ui/img/google-web_light_sq_ctn.svg");
					} else if (PivotableOAuth2UserService.PROVIDERID_GITHUB.equals(registrationId)) {
						// https://github.com/logos
						details.put("button_img", "/ui/img/GitHub_Logo.png");
					}

					registrationIdToDetails.put(registrationId, details);
				});

		if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER))) {
			registrationIdToDetails.put(IPivotableSpringProfiles.P_FAKEUSER,
					Map.of("type", "basic", "registration_id", "BASIC", "login_url", "/html/login/basic"));
		}

		return Map.of("map", registrationIdToDetails, "list", registrationIdToDetails.values());
	}

	/**
	 * 
	 * @return a REDIRECT to the relevant route to be rendered as HTML, given current user authentication status.
	 */
	@GetMapping("/html")
	public Mono<ResponseEntity<?>> loginpage() {
		// Spring-OAuth2-Login returns FOUND in case current user is not authenticated: let's follow this choice is=n
		// this route dedicated in proper direction.
		return userMayEmpty().map(user -> "login?success")
				.switchIfEmpty(Mono.just("login"))
				.map(url -> ResponseEntity.status(HttpStatus.FOUND).header(HttpHeaders.LOCATION, url).build());

	}

	// This API enables fetching the login status without getting a 401/generating a JS error/generating an exception.
	@GetMapping("/json")
	public Mono<? extends Map<String, ?>> loginStatus(@AuthenticationPrincipal OAuth2User oauth2User) {
		return userMayEmpty().map(user -> Map.of("login", HttpStatus.OK.value()))
				.switchIfEmpty(Mono.just(Map.of("login", HttpStatus.UNAUTHORIZED.value())));
	}

	// BASIC login is available for fakeUser
	@PostMapping("/basic")
	public Mono<Map> basic() {
		if (!env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER))) {
			throw new LoginRouteButNotAuthenticatedException("No BASIC");
		}

		return user().map(user -> Map.of("Authentication",
				"BASIC",
				"username",
				user.getAccountId(),
				HttpHeaders.LOCATION,
				"/html/login?success"));
	}

	private PivotableUser userFromOAuth2(OAuth2User o) {
		PivotableUserRawRaw rawRaw = PivotableOAuth2UserService.oauth2ToRawRaw(o);
		PivotableUser user = usersRegistry.getUser(rawRaw);
		return user;
	}

	// Provides the User if available. Not error if no user.
	private Mono<PivotableUser> userMayEmpty() {
		return ReactiveSecurityContextHolder.getContext().map(sc -> {
			Authentication authentication = sc.getAuthentication();

			if (authentication instanceof UsernamePasswordAuthenticationToken usernameToken) {
				// This happens on BASIC auth (for fakePlayer)
				return userFromUsername(usernameToken);
			} else if (authentication instanceof OAuth2AuthenticationToken oauth2Token) {
				// This happens on OAuth2 auth (e.g. Github login)
				return userFromOAuth2(oauth2Token.getPrincipal());
			} else {
				throw new LoginRouteButNotAuthenticatedException("Lack of authentication");
			}
		});
	}

	private Mono<PivotableUser> userOrThrow() {
		return userMayEmpty().switchIfEmpty(Mono.error(() -> new LoginRouteButNotAuthenticatedException("No user")));
	}

	@GetMapping("/user")
	public Mono<PivotableUserRaw> user() {
		return userOrThrow().map(PivotableUser::raw);
	}

	@PostMapping("/user")
	public Mono<PivotableUserRaw> user(ServerWebExchange exchange, @RequestBody PivotableUserUpdate rawUpdates) {
		return userOrThrow().flatMap(user -> {
			PivotableUser updatedUser = user;

			if (rawUpdates.getCountryCode().isPresent()) {
				String countryCode = rawUpdates.getCountryCode().get();

				PivotableUserDetails updatedDetails = user.getDetails().setCountryCode(countryCode);
				PivotableUserPreRegister preRegister =
						PivotableUserPreRegister.builder().rawRaw(user.getRawRaw()).details(updatedDetails).build();

				updatedUser = usersRegistry.registerOrUpdate(preRegister);

				log.info("accountId={} has countryCode updated {} -> {}",
						user.getAccountId(),
						user.getDetails().getCountryCode(),
						countryCode);
			}

			// return the updated user
			return Mono.just(PivotableUser.raw(updatedUser));
		});
	}

	private PivotableUser userFromUsername(UsernamePasswordAuthenticationToken usernameToken) {
		UUID accountId = UUID.fromString(usernameToken.getName());
		PivotableUser user = usersRegistry.getUser(accountId);
		return user;
	}

	@GetMapping("/oauth2/token")
	public Mono<?> token(@RequestParam(name = "refresh_token", defaultValue = "false") boolean requestRefreshToken) {
		return userOrThrow().map(user -> {
			UUID accountId = user.getAccountId();

			if (requestRefreshToken) {
				log.info("Generating an refresh_token for accountId={}", accountId);
				return kumiteTokenService.wrapInJwtRefreshToken(accountId);
			} else {
				log.info("Generating an access_token for accountId={}", user.getAccountId());
				return kumiteTokenService.wrapInJwtAccessToken(accountId);
			}
		});
	}

	// It seems much easier to return the CSRF through a dedicated API, than through Cookies and Headers, as SpringBoot
	// seems to require advanced tweaking to get it populated automatically
	// https://docs.spring.io/spring-security/reference/reactive/exploits/csrf.html#webflux-csrf-configure-custom-repository
	@GetMapping("/csrf")
	public Mono<ResponseEntity<?>> csrf(ServerWebExchange exchange) {
		Mono<CsrfToken> csrfToken = exchange.getAttribute(CsrfToken.class.getName());
		if (csrfToken == null) {
			throw new IllegalStateException("No csrfToken is available");
		}

		return csrfToken.map(csrf -> ResponseEntity.ok()
				.header(csrf.getHeaderName(), csrf.getToken())
				.body(Map.of("header", csrf.getHeaderName())));
	}

	/**
	 * @return the logout URL as a 2XX code, as 3XX can not be intercepted with Fetch API.
	 * @see https://github.com/whatwg/fetch/issues/601#issuecomment-502667208
	 */
	@GetMapping("/logout")
	public Map<String, String> logout() {
		return Map.of(HttpHeaders.LOCATION, "/html/login?logout");
	}

}