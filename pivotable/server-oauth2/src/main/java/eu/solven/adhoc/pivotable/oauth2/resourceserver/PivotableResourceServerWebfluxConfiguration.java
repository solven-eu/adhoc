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

import javax.crypto.SecretKey;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

import com.nimbusds.jose.jwk.OctetSequenceKey;

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
public class PivotableResourceServerWebfluxConfiguration {

	// https://stackoverflow.com/questions/64758305/how-to-configure-a-reactive-resource-server-to-use-a-jwt-with-a-symmetric-key
	// https://docs.spring.io/spring-security/reference/reactive/oauth2/resource-server/jwt.html
	// The browser will get an JWT given `/api/login/v1/token`. This route is protected by oauth2Login, and will
	// generate a JWT.
	// Given JWT is the only way to authenticate to the rest of the API. `oauth2ResourceServer` shall turns given
	// JWT into a proper Authentication. This is a 2-step process: JWT -> JWTClaimsSet (which will be used to make a
	// Jwt). And later a Jwt to a AbstractAuthenticationToken.
	@Bean
	@SneakyThrows(ParseException.class)
	public ReactiveJwtDecoder reactiveJwtDecoder(Environment env, IUuidGenerator uuidGenerator) {
		OctetSequenceKey octetSequenceKey = PivotableResourceServerHelpers.loadOAuth2SigningKey(env, uuidGenerator);
		SecretKey secretKey = octetSequenceKey.toSecretKey();

		return NimbusReactiveJwtDecoder.withSecretKey(secretKey)
				.macAlgorithm(PivotableResourceServerHelpers.MAC_ALGORITHM)
				.build();
	}

}
