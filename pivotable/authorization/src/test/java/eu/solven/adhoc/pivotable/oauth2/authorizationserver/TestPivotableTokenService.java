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
import java.util.UUID;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtReactiveAuthenticationManager;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.SignedJWT;

import eu.solven.adhoc.pivotable.account.internal.PivotableUser;
import eu.solven.adhoc.pivotable.account.login.IPivotableTestConstants;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.tools.JdkUuidGenerator;

public class TestPivotableTokenService {
	MockEnvironment env = new MockEnvironment();
	Supplier<PivotableTokenService> tokenService = () -> new PivotableTokenService(env, JdkUuidGenerator.INSTANCE);

	@Test
	public void testJwt_refreshToken() throws JOSEException, ParseException {
		JWK signatureSecret = PivotableTokenService.generateSignatureSecret(JdkUuidGenerator.INSTANCE);
		env.setProperty(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER, "https://some.issuer.domain");
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, signatureSecret.toJSONString());

		UUID accountId = UUID.randomUUID();
		PivotableUser user = PivotableUser.builder()
				.accountId(accountId)
				.rawRaw(IPivotableTestConstants.userRawRaw())
				.details(IPivotableTestConstants.userDetails())
				.build();
		String accessToken = tokenService.get().generateAccessToken(user.getAccountId(), Duration.ofMinutes(1), true);

		{
			JWSVerifier verifier = new MACVerifier((OctetSequenceKey) signatureSecret);

			SignedJWT signedJWT = SignedJWT.parse(accessToken);
			Assertions.assertThat(signedJWT.verify(verifier)).isTrue();
		}

		JwtReactiveAuthenticationManager authManager = new JwtReactiveAuthenticationManager(
				new PivotableResourceServerConfiguration().jwtDecoder(env, JdkUuidGenerator.INSTANCE));

		Authentication auth = authManager.authenticate(new BearerTokenAuthenticationToken(accessToken)).block();

		Assertions.assertThat(auth.getPrincipal()).isOfAnyClassIn(Jwt.class);
		Jwt jwt = (Jwt) auth.getPrincipal();
		Assertions.assertThat(jwt.getSubject()).isEqualTo(accountId.toString());
		Assertions.assertThat(jwt.getAudience()).containsExactly("Adhoc-Server");
	}

	@Test
	public void testJwt_accessToken() throws JOSEException, ParseException {
		JWK signatureSecret = PivotableTokenService.generateSignatureSecret(JdkUuidGenerator.INSTANCE);
		env.setProperty(IPivotableOAuth2Constants.KEY_OAUTH2_ISSUER, "https://some.issuer.domain");
		env.setProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY, signatureSecret.toJSONString());

		UUID accountId = UUID.randomUUID();
		PivotableUser user = PivotableUser.builder()
				.accountId(accountId)
				.rawRaw(IPivotableTestConstants.userRawRaw())
				.details(IPivotableTestConstants.userDetails())
				.build();
		String accessToken = tokenService.get().generateAccessToken(user.getAccountId(), Duration.ofMinutes(1), false);

		{
			JWSVerifier verifier = new MACVerifier((OctetSequenceKey) signatureSecret);

			SignedJWT signedJWT = SignedJWT.parse(accessToken);
			Assertions.assertThat(signedJWT.verify(verifier)).isTrue();
		}

		JwtReactiveAuthenticationManager authManager = new JwtReactiveAuthenticationManager(
				new PivotableResourceServerConfiguration().jwtDecoder(env, JdkUuidGenerator.INSTANCE));

		Authentication auth = authManager.authenticate(new BearerTokenAuthenticationToken(accessToken)).block();

		Assertions.assertThat(auth.getPrincipal()).isOfAnyClassIn(Jwt.class);
		Jwt jwt = (Jwt) auth.getPrincipal();
		Assertions.assertThat(jwt.getSubject()).isEqualTo(accountId.toString());
		Assertions.assertThat(jwt.getAudience()).containsExactly("Adhoc-Server");
	}
}
