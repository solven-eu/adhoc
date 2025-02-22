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
package eu.solven.adhoc.pivotable.security.tokens;

import java.text.ParseException;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.pivotable.account.PivotableUsersRegistry;
import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.ActiveRefreshTokens;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@AllArgsConstructor
@Slf4j
public class AccessTokenHandler {

	final PivotableTokenService kumiteTokenService;
	final PivotableUsersRegistry usersRegistry;

	final ActiveRefreshTokens activeRefreshTokens;

	// This route has to be authenticated with a refresh_token as access_token. This is not standard following OAuth2,
	// but to do it clean, we would need any way to provide a separate Authentication Server.
	public Mono<ServerResponse> getAccessToken(ServerRequest request) {
		return ReactiveSecurityContextHolder.getContext().flatMap(securityContext -> {
			Authentication authentication = securityContext.getAuthentication();

			Map.Entry<Jwt, PivotableUser> jwtToUser = userFromRefreshTokenJwt(authentication);

			AccessTokenWrapper tokenWrapper =
					kumiteTokenService.wrapInJwtAccessToken(jwtToUser.getValue().getAccountId());

			String accessTokenJti = getJti(tokenWrapper);

			log.info("Generating access_token.kid={} given refresh_token.kid={}",
					accessTokenJti,
					jwtToUser.getKey().getId());

			return ServerResponse.ok()
					.contentType(MediaType.APPLICATION_JSON)
					.body(BodyInserters.fromValue(tokenWrapper));
		});
	}

	private String getJti(AccessTokenWrapper tokenWrapper) {
		try {
			return SignedJWT.parse(tokenWrapper.getAccessToken()).getJWTClaimsSet().getJWTID();
		} catch (ParseException e) {
			throw new IllegalStateException("Issue parsing our own access_token", e);
		}
	}

	private Map.Entry<Jwt, PivotableUser> userFromRefreshTokenJwt(Authentication authentication) {
		if (!(authentication instanceof JwtAuthenticationToken jwtAuthentication)) {
			throw new IllegalArgumentException("Expected a JWT token. Was a: " + authentication.getClass());
		}

		Jwt jwt = jwtAuthentication.getToken();

		if (!jwt.getClaimAsBoolean("refresh_token")) {
			throw new IllegalArgumentException("Authenticate yourself with a refresh_token, not an access_token");
		}

		UUID accountId = UUID.fromString(jwt.getSubject());

		activeRefreshTokens.touchRefreshToken(accountId, UUID.fromString(jwt.getId()));

		PivotableUser user = usersRegistry.getUser(accountId);
		log.debug("We loaded {} from jti={}", user, jwt.getId());
		return Map.entry(jwt, user);
	}

}