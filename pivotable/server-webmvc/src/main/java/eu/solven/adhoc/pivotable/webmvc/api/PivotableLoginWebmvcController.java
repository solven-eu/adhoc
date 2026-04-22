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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.PivotableUserDetails;
import eu.solven.adhoc.pivotable.account.PivotableUserRawRaw;
import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserPreRegister;
import eu.solven.adhoc.pivotable.account.internal.PivotableUserRaw;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.security.LoginRouteButNotAuthenticatedException;
import eu.solven.adhoc.pivotable.webnone.api.IPivotableLoginConstants;
import eu.solven.adhoc.pivotable.webnone.api.PivotableUserUpdate;
import eu.solven.adhoc.pivotable.webnone.security.oauth2.PivotableOAuth2UserWebnoneService;
import graphql.com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link Controller} is dedicated to `js` usage. It bridges with the API by providing `refresh_token`s.
 *
 * @author Benoit Lacelle
 */
@RestController
@RequestMapping("/api/login/v1")
@AllArgsConstructor
@Slf4j
public class PivotableLoginWebmvcController {

	public static final String P_OAUTH2 = "adhoc.pivotable.login.oauth2.enabled";

	final ApplicationContext appContext;

	final PivotableUsersRegistry usersRegistry;
	final Environment env;

	final PivotableTokenService pivotableTokenService;

	/**
	 * @return a REDIRECT to the relevant route to be rendered as HTML, given current user authentication status.
	 */
	@GetMapping("/html")
	public ResponseEntity<?> loginpage() {
		Optional<PivotableUser> user = userMayEmpty();
		String url;
		if (user.isPresent()) {
			url = "login?success";
		} else {
			url = "login";
		}
		return ResponseEntity.status(HttpStatus.FOUND).header(HttpHeaders.LOCATION, url).build();
	}

	/**
	 * Returns login status without triggering a 401/error — useful for JS polling. When the caller is logged-in, the
	 * response is enriched with session-lifetime info so the SPA can display "session idles out in N seconds" without
	 * having to read the HttpOnly JSESSIONID cookie (which it can't).
	 *
	 * @param request
	 *            the current servlet request, used to read the existing session (never creates one).
	 * @return a JSON map with a {@code login} key holding the HTTP status code and, when logged-in, a {@code session}
	 *         sub-map describing the HTTP session lifetime.
	 */
	@GetMapping("/json")
	@ResponseBody
	public Map<String, ?> loginStatus(HttpServletRequest request) {
		Optional<PivotableUser> user = userMayEmpty();
		if (user.isPresent()) {
			// `false` so we never force-create a session just to answer a status probe.
			HttpSession session = request.getSession(false);
			if (session == null) {
				return Map.of(IPivotableLoginConstants.K_LOGIN, HttpStatus.OK.value());
			}
			return ImmutableMap.<String, Object>builder()
					.put(IPivotableLoginConstants.K_LOGIN, HttpStatus.OK.value())
					.put("session", sessionInfo(session))
					.build();
		} else {
			return Map.of(IPivotableLoginConstants.K_LOGIN, HttpStatus.UNAUTHORIZED.value());
		}
	}

	/**
	 * Describes the current {@link HttpSession} as a plain JSON-friendly map, so the SPA can render a sliding-idle
	 * countdown without reading the (HttpOnly) JSESSIONID cookie. All time fields are epoch milliseconds (UTC); the
	 * idle duration is seconds.
	 *
	 * @param session
	 *            the current HTTP session.
	 * @return a map with keys {@code maxIdleSeconds}, {@code creationEpochMs}, {@code lastAccessEpochMs}.
	 */
	private Map<String, Object> sessionInfo(HttpSession session) {
		return ImmutableMap.<String, Object>builder()
				.put("maxIdleSeconds", (long) session.getMaxInactiveInterval())
				.put("creationEpochMs", session.getCreationTime())
				.put("lastAccessEpochMs", session.getLastAccessedTime())
				.build();
	}

	/**
	 * BASIC login is available only when the {@code fakeUser} profile is active.
	 *
	 * <p>
	 * In Spring Security 6, {@code BasicAuthenticationFilter} authenticates per-request but never calls
	 * {@code SecurityContextRepository.saveContext()}, so the SecurityContext is not persisted to the HTTP session
	 * automatically. This endpoint explicitly saves it so that subsequent requests can restore authentication from the
	 * session cookie.
	 *
	 * @param request
	 *            the current servlet request, used to obtain/create the HTTP session
	 * @return authentication details for the fake user
	 */
	@PostMapping("/basic")
	@ResponseBody
	public Map<String, ?> basic(HttpServletRequest request) {
		if (!env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER))) {
			throw new LoginRouteButNotAuthenticatedException("No BASIC");
		}

		PivotableUserRaw user = user();

		// Spring Security 6 replaced SecurityContextPersistenceFilter (which auto-saved after every request) with
		// SecurityContextHolderFilter (which only loads). BasicAuthenticationFilter was never updated to call
		// saveContext() because HTTP Basic is a stateless protocol — but this endpoint deliberately bootstraps a
		// stateful session from a one-time Basic credential, so we save the context explicitly.
		// https://docs.spring.io/spring-security/reference/migration/servlet/session-management.html#_require_explicit_saving_of_securitycontextrepository
		HttpSession session = request.getSession(true);
		session.setAttribute(HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY,
				SecurityContextHolder.getContext());

		return ImmutableMap.<String, Object>builder()
				.put("Authentication", "BASIC")
				.put("username", user.getAccountId())
				.put(HttpHeaders.LOCATION, "/html/login?success")
				.build();
	}

	/**
	 * @return the raw profile of the authenticated user
	 */
	@GetMapping("/user")
	@ResponseBody
	public PivotableUserRaw user() {
		return PivotableUser.raw(userOrThrow());
	}

	/**
	 * Update the authenticated user's profile fields.
	 *
	 * @param rawUpdates
	 *            fields to update
	 * @return updated raw user profile
	 */
	@PostMapping("/user")
	@ResponseBody
	public PivotableUserRaw user(@RequestBody PivotableUserUpdate rawUpdates) {
		PivotableUser user = userOrThrow();
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

		return PivotableUser.raw(updatedUser);
	}

	/**
	 * Generate an access_token or refresh_token for the current user (browser-session authenticated).
	 *
	 * @param requestRefreshToken
	 *            if {@code true}, return a refresh_token; otherwise return an access_token
	 * @return the wrapped JWT
	 */
	@GetMapping("/oauth2/token")
	@ResponseBody
	public Object token(@RequestParam(name = "refresh_token", defaultValue = "false") boolean requestRefreshToken) {
		PivotableUser user = userOrThrow();
		UUID accountId = user.getAccountId();

		if (requestRefreshToken) {
			log.info("Generating an refresh_token for accountId={}", accountId);
			return pivotableTokenService.wrapInJwtRefreshToken(accountId);
		} else {
			log.info("Generating an access_token for accountId={}", accountId);
			return pivotableTokenService.wrapInJwtAccessToken(accountId);
		}
	}

	/**
	 * Expose the CSRF token through a dedicated endpoint since cookie/header auto-injection requires extra Spring
	 * configuration.
	 *
	 * @param request
	 *            the current servlet request, carrying the CSRF token attribute
	 * @return response with CSRF token in body and header
	 */
	@GetMapping("/csrf")
	@ResponseBody
	public ResponseEntity<?> csrf(HttpServletRequest request) {
		CsrfToken csrfToken = (CsrfToken) request.getAttribute(CsrfToken.class.getName());
		if (csrfToken == null) {
			throw new IllegalStateException("No csrfToken is available");
		}

		return ResponseEntity.ok()
				.header(csrfToken.getHeaderName(), csrfToken.getToken())
				.body(Map.of("header", csrfToken.getHeaderName()));
	}

	/**
	 * @return the logout URL as a 2XX so the Fetch API can intercept it (3XX responses cannot be intercepted).
	 * @see <a href="https://github.com/whatwg/fetch/issues/601#issuecomment-502667208">Fetch API and redirects</a>
	 */
	@GetMapping("/logout")
	@ResponseBody
	public Map<String, String> logout() {
		return Map.of(HttpHeaders.LOCATION, "/html/login?logout");
	}

	private Optional<PivotableUser> userMayEmpty() {
		Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

		if (authentication == null || !authentication.isAuthenticated()
				|| authentication instanceof AnonymousAuthenticationToken) {
			return Optional.empty();
		}

		if (authentication instanceof UsernamePasswordAuthenticationToken usernameToken) {
			return Optional.of(userFromUsername(usernameToken));
		} else if (authentication instanceof OAuth2AuthenticationToken oauth2Token) {
			return Optional.of(userFromOAuth2(oauth2Token.getPrincipal()));
		} else {
			throw new LoginRouteButNotAuthenticatedException("Lack of authentication");
		}
	}

	private PivotableUser userOrThrow() {
		return userMayEmpty().orElseThrow(() -> new LoginRouteButNotAuthenticatedException("No user"));
	}

	private PivotableUser userFromOAuth2(OAuth2User o) {
		PivotableUserRawRaw rawRaw = PivotableOAuth2UserWebnoneService.oauth2ToRawRaw(o);
		return usersRegistry.getUser(rawRaw);
	}

	private PivotableUser userFromUsername(UsernamePasswordAuthenticationToken usernameToken) {
		UUID accountId = UUID.fromString(usernameToken.getName());
		return usersRegistry.getUser(accountId);
	}

}
