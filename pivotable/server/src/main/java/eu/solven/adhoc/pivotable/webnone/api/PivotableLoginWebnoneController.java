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
package eu.solven.adhoc.pivotable.webnone.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.webnone.security.oauth2.PivotableOAuth2UserWebnoneService;
import graphql.com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link Controller} is dedicated to `js` usage. It bridges with the API by providing `refresh_token`s.
 * 
 * @author Benoit Lacelle
 *
 */
@RestController
@RequestMapping("/api/login/v1")
@RequiredArgsConstructor
@Slf4j
public class PivotableLoginWebnoneController {

	public static final String P_OAUTH2 = "adhoc.pivotable.login.oauth2.enabled";

	final ApplicationContext appContext;

	@GetMapping("/providers")
	public Map<String, ?> loginProviders() {
		Map<String, Object> registrationIdToDetails = new TreeMap<>();

		final Environment env = appContext.getEnvironment();

		if (appContext.getEnvironment().getProperty(P_OAUTH2, Boolean.class, true)) {
			// BEWARE If the following fails, you probably lacks some oauth2 registrations, as suggested in
			// application-pivotable-demo_external_oauth2.yml
			final InMemoryReactiveClientRegistrationRepository clientRegistrationRepository =
					appContext.getBean(InMemoryReactiveClientRegistrationRepository.class);

			StreamSupport.stream(clientRegistrationRepository.spliterator(), false)
					.filter(registration -> AuthorizationGrantType.AUTHORIZATION_CODE
							.equals(registration.getAuthorizationGrantType()))
					// e.g. `-Dadhoc.pivotable.login.oauth2.github.enabled=true`
					// Enabling custom deactivation as Pivotable may bring some default
					.filter(registration -> env.getProperty("adhoc.pivotable.login.oauth2.%s.enabled"
							.formatted(registration.getRegistrationId()), Boolean.class, true))
					.forEach(r -> {
						// Typically 'github' or 'google'
						String registrationId = r.getRegistrationId();
						String loginUrl = "/oauth2/authorization/%s".formatted(registrationId);

						Map<String, String> details = new LinkedHashMap<>();

						details.put("type", "oauth2");
						details.put("registration_id", registrationId);
						details.put("login_url", loginUrl);

						if (PivotableOAuth2UserWebnoneService.PROVIDERID_GOOGLE.equals(registrationId)) {
							// https://developers.google.com/identity/branding-guidelines?hl=fr
							details.put("button_img", "/ui/img/google-web_light_sq_ctn.svg");
						} else if (PivotableOAuth2UserWebnoneService.PROVIDERID_GITHUB.equals(registrationId)) {
							// https://github.com/logos
							details.put("button_img", "/ui/img/GitHub_Logo.png");
						}

						registrationIdToDetails.put(registrationId, details);
					});
		}

		if (env.acceptsProfiles(Profiles.of(IPivotableSpringProfiles.P_FAKEUSER))) {
			registrationIdToDetails.put(IPivotableSpringProfiles.P_FAKEUSER,
					ImmutableMap.builder()
							.put("type", "basic")
							.put("registration_id", IPivotableSpringProfiles.P_FAKEUSER)
							.put("login_url", "/html/login/basic")
							.build());
		}

		return Map.of("map", registrationIdToDetails, "list", registrationIdToDetails.values());
	}

}