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

import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.web.server.BearerTokenServerAuthenticationEntryPoint;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.savedrequest.NoOpServerRequestCache;

import com.nimbusds.jwt.JWT;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Default security configuration for Pivotable.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PivotableJwtWebFluxSecurity {

	/**
	 * 
	 * @param http
	 * @param env
	 * @param jwtDecoder
	 *            Knows how to check a {@link JWT}, and convert it into a {@link Jwt}. Typically provided from
	 *            {@link PivotableResourceServerConfiguration}
	 * @return
	 */
	@Order(Ordered.LOWEST_PRECEDENCE)
	@Bean
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	public SecurityWebFilterChain configureApi(Environment env,
			ReactiveJwtDecoder jwtDecoder,
			ServerHttpSecurity http) {

		boolean isFakeUser = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER));
		if (isFakeUser) {
			log.warn("{}=true", IPivotableSpringProfiles.P_FAKEUSER);
		} else {
			log.info("{}=false", IPivotableSpringProfiles.P_FAKEUSER);
		}

		// We can disable CSRF as these routes are stateless, does not rely on any cookie/session, but on some JWT
		return http

				// https://www.baeldung.com/spring-security-csrf
				.csrf(csrf -> {
					log.info("CSRF is disbled in API as API has stateless auth");
					csrf.disable();
				})
				.cors(cors -> {
					log.info("CORS is disbled in API as API has stateless auth");
					cors.disable();
				})

				.oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtDecoder(jwtDecoder)))

				// https://github.com/spring-projects/spring-security/issues/6552
				.requestCache(cache -> cache.requestCache(NoOpServerRequestCache.getInstance()))

				.authorizeExchange(auth -> auth
						// Actuator is partly public: we allow access to `/actuator` as an easy way to navigate, but not
						// to all sub-routes
						// https://docs.spring.io/spring-boot/api/rest/actuator/index.html
						.pathMatchers("/actuator", "/actuator/health/**", "/actuator/info")
						.permitAll()
						// Swagger/OpenAPI is public
						.pathMatchers("/v3/api-docs/**")
						.permitAll()
						// public API is public
						.pathMatchers(IPivotableApiConstants.PREFIX + "/public/**")
						.permitAll()

						// WebSocket: the authentication is done manually on the CONNECT frame
						.pathMatchers("/ws/**")
						.permitAll()

						// If fakeUser==true, we allow the reset route (for integration tests)
						.pathMatchers(isFakeUser ? IPivotableApiConstants.PREFIX + "/clear" : "nonono")
						.permitAll()

						// The rest needs to be authenticated
						.anyExchange()
						.authenticated())

				// Default OAuth2 behavior is to redirect to login pages
				// If not loged-in, we want to receive 401 and not 302 (which are good for UX)
				.exceptionHandling(e -> {
					BearerTokenServerAuthenticationEntryPoint authenticationEntryPoint =
							new BearerTokenServerAuthenticationEntryPoint();
					authenticationEntryPoint.setRealmName("Pivotable Realm");
					e.authenticationEntryPoint(authenticationEntryPoint);
				})

				.build();
	}

}
