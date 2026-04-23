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
package eu.solven.adhoc.pivotable.oauth2.authorizationserver;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.core.env.Environment;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.login.ITokenHolder;
import eu.solven.adhoc.pivotable.login.RefreshTokenWrapper;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerHelpers;
import eu.solven.adhoc.tools.IUuidGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages {@link ITokenHolder}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class PivotableTokenService {

	final Environment env;
	final IUuidGenerator uuidGenerator;
	// BEWARE We would prefer a RSA KeyPair for PRD
	final Supplier<OctetSequenceKey> supplierSymetricKey;

	public PivotableTokenService(Environment env, IUuidGenerator uuidgenerator) {
		this.env = env;
		this.uuidGenerator = uuidgenerator;
		this.supplierSymetricKey = this::loadSigningJwk;

		log.info("iss={}", getIssuer());
		log.info("{}.kid={}", IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, loadSigningJwk().getKeyID());
	}

	@SneakyThrows({ IllegalStateException.class, ParseException.class })
	private OctetSequenceKey loadSigningJwk() {
		return PivotableResourceServerHelpers.loadOAuth2SigningKey(env, uuidGenerator);
	}

	public String generateAccessToken(UUID accountId, Duration accessTokenValidity, boolean isRefreshToken) {
		// Generating a Signed JWT
		// https://auth0.com/blog/rs256-vs-hs256-whats-the-difference/
		// https://security.stackexchange.com/questions/194830/recommended-asymmetric-algorithms-for-jwt
		// https://curity.io/resources/learn/jwt-best-practices/
		JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(PivotableResourceServerHelpers.MAC_ALGORITHM.getName());
		JWSHeader.Builder headerBuilder = new JWSHeader.Builder(jwsAlgorithm).type(JOSEObjectType.JWT);

		Instant now = Instant.now();

		String issuer = getIssuer();
		JWTClaimsSet.Builder claimsSetBuilder = new JWTClaimsSet.Builder().subject(accountId.toString())
				.audience("Pivotable-Server")
				// https://connect2id.com/products/server/docs/api/token#url
				.issuer(issuer)
				// https://www.oauth.com/oauth2-servers/access-tokens/self-encoded-access-tokens/
				// This JWTId is very important, as it could be used to ban some access_token used by some lost VM
				// running some bot.
				.jwtID(uuidGenerator.randomUUID().toString())
				.issueTime(Date.from(now))
				.notBeforeTime(Date.from(now))
				.expirationTime(Date.from(now.plus(accessTokenValidity)))
				.claim("refresh_token", isRefreshToken);

		SignedJWT signedJWT = new SignedJWT(headerBuilder.build(), claimsSetBuilder.build());

		try {
			JWSSigner signer = new MACSigner(supplierSymetricKey.get());
			signedJWT.sign(signer);
		} catch (JOSEException e) {
			throw new IllegalStateException("Issue signing the JWT", e);
		}

		return signedJWT.serialize();
	}

	private String getIssuer() {
		String issuerBaseUrl = env.getRequiredProperty(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER);
		if ("NEEDS_TO_BE_DEFINED".equals(issuerBaseUrl)) {
			throw new IllegalStateException("Need to setup %s".formatted(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER));
		}
		// This matches `/api/v1/oauth2/token` as route for token generation
		return issuerBaseUrl + IPivotableApiConstants.PREFIX + "/oauth2";
	}

	/**
	 * Generates an access token corresponding to provided user entity based on configured settings. The generated
	 * access token can be used to perform tasks on behalf of the user on subsequent HTTP calls to the application until
	 * it expires or is revoked.
	 * 
	 * @param accountId
	 *            The user for whom to generate an access token.
	 * @throws IllegalArgumentException
	 *             if provided argument is <code>null</code>.
	 * @return The generated JWT access token.
	 * @throws IllegalStateException
	 */
	public AccessTokenWrapper wrapInJwtAccessToken(UUID accountId) {
		// access_token are short-lived. Overridable via config so test profiles
		// (e.g. pivotable-e2e-shorttoken) can force near-instant expiry.
		// Read as String + Duration.parse so we don't depend on Spring Boot's
		// ApplicationConversionService being registered on the Environment
		// (e.g. MockEnvironment in unit tests does not have it).
		Duration accessTokenValidity =
				Duration.parse(env.getProperty(IPivotableOAuth2Constants.KEY_ACCESS_TOKEN_VALIDITY,
						IPivotableOAuth2Constants.DEFAULT_ACCESS_TOKEN_VALIDITY));

		String accessToken = generateAccessToken(accountId, accessTokenValidity, false);

		// https://www.oauth.com/oauth2-servers/access-tokens/access-token-response/
		return AccessTokenWrapper.builder()
				.accessToken(accessToken)
				.tokenType("Bearer")
				.expiresIn(accessTokenValidity.toSeconds())
				.build();

	}

	// https://stackoverflow.com/questions/38986005/what-is-the-purpose-of-a-refresh-token
	// https://stackoverflow.com/questions/40555855/does-the-refresh-token-expire-and-if-so-when
	public RefreshTokenWrapper wrapInJwtRefreshToken(UUID accountId) {
		// refresh_token are long-lived. Overridable via config so test profiles
		// can force a short-lived refresh_token and exercise the "must re-login" path.
		// Read as String + Duration.parse so we don't depend on Spring Boot's
		// ApplicationConversionService being registered on the Environment.
		Duration refreshTokenValidity =
				Duration.parse(env.getProperty(IPivotableOAuth2Constants.KEY_REFRESH_TOKEN_VALIDITY,
						IPivotableOAuth2Constants.DEFAULT_REFRESH_TOKEN_VALIDITY));

		String accessToken = generateAccessToken(accountId, refreshTokenValidity, true);

		// https://www.oauth.com/oauth2-servers/access-tokens/access-token-response/
		return RefreshTokenWrapper.builder()
				.refreshToken(accessToken)
				.tokenType("Bearer")
				.expiresIn(refreshTokenValidity.toSeconds())
				.build();

	}

}
