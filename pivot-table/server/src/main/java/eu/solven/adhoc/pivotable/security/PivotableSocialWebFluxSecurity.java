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
package eu.solven.adhoc.pivotable.security;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.authentication.UserDetailsRepositoryReactiveAuthenticationManager;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity.HttpBasicSpec;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcReactiveOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.client.userinfo.ReactiveOAuth2UserService;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.RedirectServerAuthenticationFailureHandler;
import org.springframework.security.web.server.authentication.RedirectServerAuthenticationSuccessHandler;
import org.springframework.security.web.server.authentication.logout.RedirectServerLogoutSuccessHandler;
import org.springframework.security.web.server.context.WebSessionServerSecurityContextRepository;
import org.springframework.security.web.server.csrf.WebSessionServerCsrfTokenRepository;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.security.oauth2.PivotableOAuth2UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@EnableWebFluxSecurity
@Import({

		PivotableOAuth2UserService.class,

})
@RequiredArgsConstructor
@Slf4j
public class PivotableSocialWebFluxSecurity {

	// https://github.com/spring-projects/spring-security/issues/15846
	@Bean
	public OidcReactiveOAuth2UserService oidcReactiveOAuth2UserService(
			ReactiveOAuth2UserService<OAuth2UserRequest, OAuth2User> oauth2UserService) {
		OidcReactiveOAuth2UserService oidcReactiveOAuth2UserService = new OidcReactiveOAuth2UserService();

		oidcReactiveOAuth2UserService.setOauth2UserService(oauth2UserService);

		return oidcReactiveOAuth2UserService;
	}

	// https://github.com/ch4mpy/spring-addons/tree/master/samples/tutorials/resource-server_with_ui
	// https://stackoverflow.com/questions/74744901/default-401-instead-of-redirecting-for-oauth2-login-spring-security
	// `-1` as this has to be used in priority aver the API securityFilterChain
	@Order(Ordered.LOWEST_PRECEDENCE - 1)
	@Bean
	public SecurityWebFilterChain configureUi(ServerHttpSecurity http, Environment env) {

		boolean isFakeUser = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER));
		if (isFakeUser) {
			log.warn("{}=true", IPivotableSpringProfiles.P_FAKEUSER);
		} else {
			log.info("{}=false", IPivotableSpringProfiles.P_FAKEUSER);
		}

		return http
				// We restrict the scope of this UI securityFilterChain to UI routes
				// Not matching routes will be handled by the API securityFilterChain
				.securityMatcher(ServerWebExchangeMatchers.pathMatchers(
						// The `/api/login/v1/**` routes are authenticated through browser session, build from OAuth2
						// provider
						"/api/login/v1/**",
						"/oauth2/**",

						// The logout route (do a POST to logout, i.e. clear the session)
						"/logout",

						// Holds static resources (e.g. `/ui/js/store.js`)
						"/ui/js/**",
						"/ui/img/**",
						// The routes used by the spa
						"/",
						"/favicon.ico",
						"/html/**",

						"/login/oauth2/code/*",

						"/swagger-ui.html",
						"/swagger-ui/**",
						"/webjars/**"
				// ,

				// WebSocket is this relevant given the URL starts with "ws://"
				// "/gs-guide-websocket",
				// "/gs-guide-websocket/**",
				// "/ws/**"
				))

				.csrf(csrf -> {
					csrf
							// https://docs.spring.io/spring-security/reference/reactive/exploits/csrf.html#webflux-csrf-configure-custom-repository
							// .csrfTokenRepository(CookieServerCsrfTokenRepository.withHttpOnlyFalse())

							// This will NOT provide the CSRF as a header: `X-CSRF-TOKEN`
							// But it wlll help making it available on `/api/login/v1/csrf`
							.csrfTokenRepository(new WebSessionServerCsrfTokenRepository());
				})

				.authorizeExchange(auth -> auth

						// WebSocket: the authentication is done manually on the CONNECT frame
						// .pathMatchers("/gs-guide-websocket", "/gs-guide-websocket/**", "/ws/**")
						// .permitAll()

						// Login does not requires being loggged-in yet
						.pathMatchers("/login/oauth2/code/**")
						.permitAll()

						// Swagger UI
						.pathMatchers("/swagger-ui.html", "/swagger-ui/**")
						.permitAll()

						// The route used by the SPA: they all serve index.html
						.pathMatchers("/", "/html/**")
						.permitAll()

						// Webjars and static resources
						.pathMatchers("/ui/js/**", "/ui/img/**", "/webjars/**", "/favicon.ico")
						.permitAll()

						// If there is no logged-in user, we return a 401.
						// `permitAll` is useful to return a 401 manually, else `.oauth2Login` would return a 302
						.pathMatchers("/api/login/v1/json",
								// `BASIC` should be added here only if fakeUser
								"/api/login/v1/basic",
								"/api/login/v1/user",
								"/api/login/v1/oauth2/token",
								"/api/login/v1/html",
								"/api/login/v1/providers",
								"/api/login/v1/csrf",
								"/api/login/v1/logout")
						.permitAll()

						// The rest needs to be authenticated
						.anyExchange()
						.authenticated())

				// `/html/login` has to be synced with the SPA login route
				.formLogin(login -> {
					ReactiveAuthenticationManager ram = auth -> {
						throw new IllegalStateException();
					};

					String loginPage = "/html/login";
					login.loginPage(loginPage)
							// Required not to get an NPE at `.build()`
							.authenticationManager(ram);
				})
				// How to request prompt=consent for Github?
				// https://docs.spring.io/spring-security/reference/servlet/oauth2/client/authorization-grants.html
				// https://stackoverflow.com/questions/74242738/how-to-logout-from-oauth-signed-in-web-app-with-github
				.oauth2Login(oauth2 -> {
					String loginSuccess = "/html/login?success";
					oauth2.authenticationSuccessHandler(new RedirectServerAuthenticationSuccessHandler(loginSuccess));

					String loginError = "/html/login?error";
					oauth2.authenticationFailureHandler(new RedirectServerAuthenticationFailureHandler(loginError));
				})
				// .oauth2Client(oauth2 -> oauth2.)

				.httpBasic(basic -> {
					if (isFakeUser) {
						configureBasicForFakeUser(basic);
					} else {
						basic.disable();
					}
				})

				.logout(logout -> {
					RedirectServerLogoutSuccessHandler logoutSuccessHandler = new RedirectServerLogoutSuccessHandler();
					// We need to redirect to a 2XX URL, and not a 3XX URL, as Fetch API can not intercept redirections.
					logoutSuccessHandler.setLogoutSuccessUrl(URI.create("/api/login/v1/logout"));
					logout.logoutSuccessHandler(logoutSuccessHandler);
				})

				.build();
	}

	private void configureBasicForFakeUser(HttpBasicSpec basic) {
		Map<String, UserDetails> userDetails = new ConcurrentHashMap<>();

		UserDetails fakeUser = User.builder()
				.username(FakeUser.ACCOUNT_ID.toString())
				// `{noop}` relates with `PasswordEncoderFactories.createDelegatingPasswordEncoder()`
				.password("{noop}" + "no_password")
				.roles(IPivotableSpringProfiles.P_FAKEUSER)
				.build();

		userDetails.put(fakeUser.getUsername(), fakeUser);

		UserDetailsRepositoryReactiveAuthenticationManager ram =
				new UserDetailsRepositoryReactiveAuthenticationManager(new MapReactiveUserDetailsService(userDetails));
		basic.authenticationManager(ram).securityContextRepository(new WebSessionServerSecurityContextRepository());
	}

}
