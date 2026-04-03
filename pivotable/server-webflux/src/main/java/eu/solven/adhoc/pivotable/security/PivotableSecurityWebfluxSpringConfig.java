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

import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;

import eu.solven.adhoc.pivotable.account.JwtUserContextHolder;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableJwtWebfluxSecurity;
import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerConfiguration;
import eu.solven.adhoc.pivotable.webflux.PivotableWebExceptionHandler;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginWebfluxController;
import eu.solven.adhoc.pivotable.webnone.api.GreetingController;
import eu.solven.adhoc.pivotable.webnone.api.PivotableMetadataController;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSecurityWebnoneSpringConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Everything related with security in Pivotable.
 * 
 * @author Benoit Lacelle
 */
// https://docs.spring.io/spring-security/reference/reactive/oauth2/login/advanced.html#webflux-oauth2-login-advanced-userinfo-endpoint
@EnableWebFluxSecurity
@Import({

		// none
		PivotableSecurityWebnoneSpringConfig.class,

		// webflux
		PivotableSocialWebfluxSecurity.class,
		PivotableJwtWebfluxSecurity.class,

		GreetingController.class,
		PivotableLoginWebfluxController.class,
		PivotableMetadataController.class,

		PivotableResourceServerConfiguration.class,
		PivotableTokenService.class,

		JwtUserContextHolder.class,

		PivotableWebExceptionHandler.class,

})
@Slf4j
public class PivotableSecurityWebfluxSpringConfig {

}