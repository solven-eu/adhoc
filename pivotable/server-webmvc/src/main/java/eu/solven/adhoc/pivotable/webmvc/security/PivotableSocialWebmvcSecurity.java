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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserService;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import org.springframework.security.web.csrf.HttpSessionCsrfTokenRepository;
import org.springframework.security.web.server.SecurityWebFilterChain;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.account.fake_user.FakeUser;
import eu.solven.adhoc.pivotable.webmvc.security.oauth2.PivotableOAuth2UserService;
import eu.solven.adhoc.pivotable.webnone.api.PivotableLoginWebnoneController;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSocialWebnoneSecurity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable authentication with social identity providers (e.g. GitHub, Google).
 * 
 * @author Benoit Lacelle
 */
@Import({

		PivotableSocialWebnoneSecurity.class,

		PivotableOAuth2UserService.class,

})
@RequiredArgsConstructor
@Slf4j
public class PivotableSocialWebmvcSecurity {
	@Autowired
	ApplicationContext appContext;

	// https://github.com/spring-projects/spring-security/issues/15846
	@Bean
	public OidcUserService oidcOAuth2UserService(OAuth2UserService<OAuth2UserRequest, OAuth2User> oauth2UserService) {
		OidcUserService oidcReactiveOAuth2UserService = new OidcUserService();

		oidcReactiveOAuth2UserService.setOauth2UserService(oauth2UserService);

		return oidcReactiveOAuth2UserService;
	}

	/**
	 * 
	 * @param http
	 * @param env
	 * @return a {@link SecurityWebFilterChain} related to social identity providers.
	 */
	// https://github.com/ch4mpy/spring-addons/tree/master/samples/tutorials/resource-server_with_ui
	// https://stackoverflow.com/questions/74744901/default-401-instead-of-redirecting-for-oauth2-login-spring-security
	// `-1` as this has to be used in priority aver the API securityFilterChain
	@Order(Ordered.LOWEST_PRECEDENCE - 1)
	@Bean
	public SecurityFilterChain configureUi(HttpSecurity http, Environment env) {

		boolean isFakeUser = env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER));
		if (isFakeUser) {
			log.warn("{}=true", IPivotableSpringProfiles.P_FAKEUSER);
		} else {
			log.info("{}=false", IPivotableSpringProfiles.P_FAKEUSER);
		}

		HttpSecurity commonConf = http
				// We restrict the scope of this UI securityFilterChain to UI routes
				// Not matching routes will be handled by the API securityFilterChain
				.securityMatcher(
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
				)

				.csrf(csrf -> {
					csrf
							// https://docs.spring.io/spring-security/reference/reactive/exploits/csrf.html#webflux-csrf-configure-custom-repository
							// .csrfTokenRepository(CookieServerCsrfTokenRepository.withHttpOnlyFalse())

							// This will NOT provide the CSRF as a header: `X-CSRF-TOKEN`
							// But it wlll help making it available on `/api/login/v1/csrf`
							.csrfTokenRepository(new HttpSessionCsrfTokenRepository());
				})

				.authorizeHttpRequests(auth -> auth

						// WebSocket: the authentication is done manually on the CONNECT frame
						// .pathMatchers("/gs-guide-websocket", "/gs-guide-websocket/**", "/ws/**")
						// .permitAll()

						// Login does not requires being loggged-in yet
						.requestMatchers("/login/oauth2/code/**")
						.permitAll()

						// Swagger UI
						.requestMatchers("/swagger-ui.html", "/swagger-ui/**")
						.permitAll()

						// The route used by the SPA: they all serve index.html
						.requestMatchers("/", "/html/**")
						.permitAll()

						// Webjars and static resources
						.requestMatchers("/ui/js/**", "/ui/img/**", "/webjars/**", "/favicon.ico")
						.permitAll()

						// PivotableLoginController
						// If there is no logged-in user, we return a 401.
						// `permitAll` is useful to return a 401 manually, else `.oauth2Login` would return a 302
						.requestMatchers(
								// `/json` returns a custom/explicit JSON in case of 401
								"/api/login/v1/json",
								// `/providers` are public as we need to see available providers before being logged-in
								"/api/login/v1/providers",
								// `/csrf` deserves being provided even for anonymous user
								// https://stackoverflow.com/questions/30767893/does-an-anonymous-comment-post-form-need-csrf-token-if-not-why-does-so-use-it-a
								"/api/login/v1/csrf",
								// if not logged in, we redirect to the login URL
								// if logged in, we redirect to the loginSuccess URL
								"/api/login/v1/html",
								// see TestSecurity_WithOAuth2_asOAuth2User.testLogout() for the workflow
								"/api/login/v1/logout")
						.permitAll()

						.requestMatchers(
								// // `BASIC` should be added here only if fakeUser
								"/api/login/v1/basic",
								"/api/login/v1/user",
								"/api/login/v1/oauth2/token")
						.authenticated()

						// The rest needs to be authenticated
						.anyRequest()
						.authenticated())

				// `/html/login` has to be synced with the SPA login route
				.formLogin(login -> {
					ReactiveAuthenticationManager ram = auth -> {
						throw new IllegalStateException();
					};

					String loginPage = "/html/login";
					login.loginPage(loginPage)
					// Required not to get an NPE at `.build()`
					// .authenticationManager(ram)
					;
				})

				.httpBasic(basic -> {
					if (isFakeUser) {
						basic.realmName("Pivotable BASIC Login Realm");
					} else {
						basic.disable();
					}
				})

				.logout(logout -> {
					SimpleUrlLogoutSuccessHandler logoutSuccessHandler = new SimpleUrlLogoutSuccessHandler();
					// We need to redirect to a 2XX URL, and not a 3XX URL, as Fetch API can not intercept redirections.
					logoutSuccessHandler.setDefaultTargetUrl("/api/login/v1/logout");
					logout.logoutSuccessHandler(logoutSuccessHandler);
				})

				.exceptionHandling(e -> {
					BearerTokenAuthenticationEntryPoint authenticationEntryPoint =
							new BearerTokenAuthenticationEntryPoint();
					authenticationEntryPoint.setRealmName("Pivotable Login Realm");
					e.authenticationEntryPoint(authenticationEntryPoint);
				});

		if (isFakeUser) {
			commonConf = configureBasicForFakeUser(commonConf);
		}

		if (env.getProperty(PivotableLoginWebnoneController.P_OAUTH2, Boolean.class, true)) {
			commonConf = commonConf
					// How to request prompt=consent for Github?
					// https://docs.spring.io/spring-security/reference/servlet/oauth2/client/authorization-grants.html
					// https://stackoverflow.com/questions/74242738/how-to-logout-from-oauth-signed-in-web-app-with-github
					.oauth2Login(oauth2 -> {
						String loginSuccess = "/html/login?success";
						oauth2.successHandler(new SimpleUrlAuthenticationSuccessHandler(loginSuccess));

						String loginError = "/html/login?error";
						oauth2.failureHandler(new SimpleUrlAuthenticationFailureHandler(loginError));
					})
			// .oauth2Client(oauth2 -> oauth2.)
			;
		}

		return commonConf

				.build();
	}

	// `java:S6437` is about the hardcoded `no_password`, which is safe as this activates only with the
	// `IPivotableSpringProfiles.P_FAKEUSER` profile
	@SuppressWarnings("java:S6437")
	private HttpSecurity configureBasicForFakeUser(HttpSecurity commonConf) {
		Map<String, UserDetails> userDetails = new ConcurrentHashMap<>();

		UserDetails fakeUser = User.builder()
				.username(FakeUser.ACCOUNT_ID.toString())
				// `{noop}` relates with `PasswordEncoderFactories.createDelegatingPasswordEncoder()`
				.password("{noop}" + "no_password")
				.roles(IPivotableSpringProfiles.P_FAKEUSER)
				.build();

		userDetails.put(fakeUser.getUsername(), fakeUser);

		return commonConf.userDetailsService(new InMemoryUserDetailsManager(fakeUser));
	}
}
