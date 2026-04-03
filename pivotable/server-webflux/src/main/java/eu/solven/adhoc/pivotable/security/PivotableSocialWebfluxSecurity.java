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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcReactiveOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.client.userinfo.ReactiveOAuth2UserService;
import org.springframework.security.oauth2.core.user.OAuth2User;

import eu.solven.adhoc.pivotable.security.oauth2.PivotableReactiveOAuth2UserService;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSocialWebnoneSecurity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable authentication with social identity providers (e.g. GitHub, Google).
 * 
 * @author Benoit Lacelle
 */
@EnableWebFluxSecurity
@Import({

		PivotableReactiveOAuth2UserService.class,

		PivotableSocialWebnoneSecurity.class,

})
@RequiredArgsConstructor
@Slf4j
public class PivotableSocialWebfluxSecurity {
	@Autowired
	ApplicationContext appContext;

	// https://github.com/spring-projects/spring-security/issues/15846
	@Bean
	public OidcReactiveOAuth2UserService oidcReactiveOAuth2UserService(
			ReactiveOAuth2UserService<OAuth2UserRequest, OAuth2User> oauth2UserService) {
		OidcReactiveOAuth2UserService oidcReactiveOAuth2UserService = new OidcReactiveOAuth2UserService();

		oidcReactiveOAuth2UserService.setOauth2UserService(oauth2UserService);

		return oidcReactiveOAuth2UserService;
	}

}
