/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import eu.solven.adhoc.pivotable.oauth2.resourceserver.PivotableResourceServerWebmvcConfiguration;
import eu.solven.adhoc.pivotable.webmvc.api.PivotableLoginWebmvcController;
import eu.solven.adhoc.pivotable.webmvc.security.tokens.AccessTokenController;
import eu.solven.adhoc.pivotable.webnone.security.PivotableSecurityWebnoneSpringConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Everything related with security in Pivotable.
 * 
 * @author Benoit Lacelle
 */
// https://docs.spring.io/spring-security/reference/reactive/oauth2/login/advanced.html#webflux-oauth2-login-advanced-userinfo-endpoint
@EnableWebSecurity
@Import({

		// none
		PivotableSecurityWebnoneSpringConfig.class,

		// webmvc
		PivotableSocialWebmvcSecurity.class,
		PivotableJwtWebmvcSecurity.class,
		PivotableResourceServerWebmvcConfiguration.class,

		PivotableLoginWebmvcController.class,
		AccessTokenController.class,

})
@Slf4j
public class PivotableSecurityWebmvcSpringConfig {

}