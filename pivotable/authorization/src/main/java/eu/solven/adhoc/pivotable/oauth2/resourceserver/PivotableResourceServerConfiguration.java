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
package eu.solven.adhoc.pivotable.oauth2.resourceserver;

import java.text.ParseException;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

import com.nimbusds.jose.jwk.OctetSequenceKey;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.oauth2.IPivotableOAuth2Constants;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.tools.IUuidGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages decoding of JWT.
 * 
 * @author Benoit Lacelle
 */
@Configuration
@Slf4j
public class PivotableResourceServerConfiguration {

	public static final MacAlgorithm MAC_ALGORITHM = MacAlgorithm.HS256;

	private static final AtomicReference<String> GENERATED_SIGNINGKEY = new AtomicReference<>();

	// https://stackoverflow.com/questions/64758305/how-to-configure-a-reactive-resource-server-to-use-a-jwt-with-a-symmetric-key
	// https://docs.spring.io/spring-security/reference/reactive/oauth2/resource-server/jwt.html
	// The browser will get an JWT given `/api/login/v1/token`. This route is protected by oauth2Login, and will
	// generate a JWT.
	// Given JWT is the only way to authenticate to the rest of the API. `oauth2ResourceServer` shall turns given
	// JWT into a proper Authentication. This is a 2-step process: JWT -> JWTClaimsSet (which will be used to make a
	// Jwt). And later a Jwt to a AbstractAuthenticationToken.
	@Bean
	@SneakyThrows(ParseException.class)
	public ReactiveJwtDecoder jwtDecoder(Environment env, IUuidGenerator uuidGenerator) {
		OctetSequenceKey octetSequenceKey = loadOAuth2SigningKey(env, uuidGenerator);
		SecretKey secretKey = octetSequenceKey.toSecretKey();

		return NimbusReactiveJwtDecoder.withSecretKey(secretKey).macAlgorithm(MAC_ALGORITHM).build();
	}

	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	public static OctetSequenceKey loadOAuth2SigningKey(Environment env, IUuidGenerator uuidGenerator)
			throws ParseException {
		String secretKeySpec = env.getRequiredProperty(IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY);

		if ("NEEDS_TO_BE_DEFINED".equals(secretKeySpec)) {
			throw new IllegalStateException("Lack proper `" + IPivotableOAuth2Constants.KEY_JWT_SIGNINGKEY
					+ "` or spring.profiles.active="
					+ IPivotableSpringProfiles.P_UNSAFE_SERVER);
		} else if (IPivotableOAuth2Constants.GENERATE.equals(secretKeySpec)) {
			if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_PRDMODE))) {
				throw new IllegalStateException("Can not GENERATE oauth2 signingKey in `prodmode`");
			}
			// Ensure we generate a signingKey only once, so that the key in IJwtDecoder and the token in Bearer token
			// are based on the same signingKey
			synchronized (PivotableResourceServerConfiguration.class) {
				if (GENERATED_SIGNINGKEY.get() == null) {
					log.warn("We generate a random signingKey");
					secretKeySpec = PivotableTokenService.generateSignatureSecret(uuidGenerator).toJSONString();
					GENERATED_SIGNINGKEY.set(secretKeySpec);
				} else {
					secretKeySpec = GENERATED_SIGNINGKEY.get();
				}
			}
		}

		OctetSequenceKey octetSequenceKey = OctetSequenceKey.parse(secretKeySpec);
		log.info("Loaded or generated octetSequenceKey with kid={}", octetSequenceKey.getKeyID());
		return octetSequenceKey;
	}
}
