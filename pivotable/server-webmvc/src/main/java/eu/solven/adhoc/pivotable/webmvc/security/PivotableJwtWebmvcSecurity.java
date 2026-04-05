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
package eu.solven.adhoc.pivotable.webmvc.security;

import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.CsrfConfigurer;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.savedrequest.NullRequestCache;

import com.nimbusds.jwt.JWT;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * Default WebMVC security configuration for Pivotable. Configures the catch-all JWT resource-server filter chain that
 * protects {@code /api/v1/**} routes and declares the permit-all exceptions (actuator, OpenAPI, public API).
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class PivotableJwtWebmvcSecurity {

	/**
	 * Catch-all {@link SecurityFilterChain} that applies JWT bearer authentication to every request not matched by a
	 * higher-priority chain. Permit-all exceptions are registered for actuator health, OpenAPI docs, and the
	 * {@code /api/v1/public/**} prefix.
	 *
	 * @param env
	 *            used to detect the {@code P_FAKEUSER} profile
	 * @param jwtDecoder
	 *            Knows how to check a {@link JWT}, and convert it into a {@link Jwt}. Typically provided from
	 *            {@code PivotableResourceServerWebmvcConfiguration}
	 * @param http
	 *            the {@link HttpSecurity} builder
	 * @return the configured filter chain
	 */
	@Order(Ordered.LOWEST_PRECEDENCE)
	@Bean
	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	public SecurityFilterChain configureApi(Environment env, JwtDecoder jwtDecoder, HttpSecurity http) {
		boolean isFakeUser = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER));
		if (isFakeUser) {
			log.warn("{}=true", IPivotableSpringProfiles.P_FAKEUSER);
		} else {
			log.info("{}=false", IPivotableSpringProfiles.P_FAKEUSER);
		}

		// We can disable CSRF as these routes are stateless, does not rely on any cookie/session, but on some JWT
		return http

				// https://www.baeldung.com/spring-security-csrf
				.csrf(this::onCsrf)
				.cors(cors -> {
					log.info("CORS is disbled in API as API has stateless auth");
					cors.disable();
				})

				.oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.decoder(jwtDecoder)))

				// https://github.com/spring-projects/spring-security/issues/6552
				.requestCache(cache -> cache.requestCache(new NullRequestCache()))

				.authorizeHttpRequests(auth -> auth
						// Actuator is partly public: we allow access to `/actuator` as an easy way to navigate, but not
						// to all sub-routes
						// https://docs.spring.io/spring-boot/api/rest/actuator/index.html
						.requestMatchers("/actuator", "/actuator/health/**", "/actuator/info")
						.permitAll()
						// Swagger/OpenAPI is public
						.requestMatchers("/v3/api-docs/**")
						.permitAll()
						// public API is public
						.requestMatchers(IPivotableApiConstants.PREFIX + "/public/**")
						.permitAll()

						// WebSocket: the authentication is done manually on the CONNECT frame
						.requestMatchers("/ws/**")
						.permitAll()

						// If fakeUser==true, we allow the reset route (for integration tests)
						.requestMatchers(isFakeUser ? IPivotableApiConstants.PREFIX + "/clear" : "/nonono")
						.permitAll()

						// The rest needs to be authenticated
						.anyRequest()
						.authenticated())

				// Default OAuth2 behavior is to redirect to login pages
				// If not logged-in, we want to receive 401 and not 302 (which are good for UX while this covers API)
				.exceptionHandling(e -> {
					BearerTokenAuthenticationEntryPoint authenticationEntryPoint =
							new BearerTokenAuthenticationEntryPoint();
					authenticationEntryPoint.setRealmName("Pivotable API Realm");
					e.authenticationEntryPoint(authenticationEntryPoint);
				})

				.build();
	}

	/**
	 * Disables CSRF protection on the API filter chain. Safe because the API is stateless (JWT bearer, no cookies).
	 *
	 * @param csrf
	 *            the CSRF configurer to disable
	 */
	@SuppressFBWarnings(value = "SPRING_CSRF_PROTECTION_DISABLED", justification = "This resource is stateless.")
	protected void onCsrf(CsrfConfigurer<HttpSecurity> csrf) {
		log.info("CSRF is disbled in API as API has stateless auth");
		csrf.disable();
	}

}
