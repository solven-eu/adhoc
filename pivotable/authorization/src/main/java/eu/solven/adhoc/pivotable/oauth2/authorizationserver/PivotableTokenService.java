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
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.gen.OctetSequenceKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.pivotable.login.AccessTokenWrapper;
import eu.solven.adhoc.pivotable.login.ITokenHolder;
import eu.solven.adhoc.pivotable.login.RefreshTokenWrapper;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import eu.solven.adhoc.tools.IUuidGenerator;
import eu.solven.adhoc.tools.JdkUuidGenerator;
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
		return PivotableResourceServerConfiguration.loadOAuth2SigningKey(env, uuidGenerator);
	}

	public static void main(String[] args) {
		JWK secretKey = generateSignatureSecret(new JdkUuidGenerator());
		log.info("Secret key for JWT signing: {}", secretKey.toJSONString());
	}

	@SneakyThrows(JOSEException.class)
	// @SuppressWarnings("PMD.ReplaceJavaUtilDate")
	public static JWK generateSignatureSecret(IUuidGenerator uuidGenerator) {
		// https://connect2id.com/products/nimbus-jose-jwt/examples/jws-with-hmac
		// Generate random 256-bit (32-byte) shared secret
		// SecureRandom random = new SecureRandom();
		//
		String rawNbBits = PivotableResourceServerConfiguration.MAC_ALGORITHM.getName().substring("HS".length());
		int nbBits = Integer.parseInt(rawNbBits);

		OctetSequenceKey jwk = new OctetSequenceKeyGenerator(nbBits).keyID(uuidGenerator.randomUUID().toString())
				.algorithm(JWSAlgorithm.parse(PivotableResourceServerConfiguration.MAC_ALGORITHM.getName()))
				.issueTime(Date.from(Instant.now()))
				.generate();

		log.info("Generated a JWK with kid={}", jwk.getKeyID());

		return jwk;
	}

	public String generateAccessToken(UUID accountId, Duration accessTokenValidity, boolean isRefreshToken) {

		// Generating a Signed JWT
		// https://auth0.com/blog/rs256-vs-hs256-whats-the-difference/
		// https://security.stackexchange.com/questions/194830/recommended-asymmetric-algorithms-for-jwt
		// https://curity.io/resources/learn/jwt-best-practices/
		JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(PivotableResourceServerConfiguration.MAC_ALGORITHM.getName());
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
		// access_token are short-lived
		Duration accessTokenValidity = Duration.parse("PT1H");

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
		// refresh_token are long-lived
		Duration refreshTokenValidity = Duration.parse("P365D");

		String accessToken = generateAccessToken(accountId, refreshTokenValidity, true);

		// https://www.oauth.com/oauth2-servers/access-tokens/access-token-response/
		return RefreshTokenWrapper.builder()
				.refreshToken(accessToken)
				.tokenType("Bearer")
				.expiresIn(refreshTokenValidity.toSeconds())
				.build();

	}

}
