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
package eu.solven.adhoc.pivotable.webflux;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import eu.solven.adhoc.beta.api.AdhocSchemaForApi;
import eu.solven.adhoc.pivotable.cube.CubesHandler;
import eu.solven.adhoc.pivotable.entrypoint.EntrypointsHandler;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.ActiveRefreshTokens;
import eu.solven.adhoc.pivotable.oauth2.authorizationserver.PivotableTokenService;
import eu.solven.adhoc.pivotable.query.QueryRequestHandler;
import eu.solven.adhoc.pivotable.security.tokens.AccessTokenHandler;
import eu.solven.adhoc.pivotable.webflux.api.AdhocApiRouter;
import eu.solven.adhoc.pivotable.webflux.api.AdhocSpaRouter;
import eu.solven.adhoc.pivotable.webflux.api.GreetingHandler;
import eu.solven.adhoc.pivotable.webflux.api.KumiteFakeUserRouter;
import eu.solven.adhoc.pivotable.webflux.api.PivotableLoginRouter;
import lombok.extern.slf4j.Slf4j;

@Import({

		AdhocSpaRouter.class,

		AdhocApiRouter.class,
		GreetingHandler.class,

		KumiteFakeUserRouter.class,

		GreetingHandler.class,
		EntrypointsHandler.class,
		CubesHandler.class,
		QueryRequestHandler.class,
		// AdhocSchemaForApi.class,

		PivotableLoginRouter.class,
		AccessTokenHandler.class,
		ActiveRefreshTokens.class,

		PivotableWebExceptionHandler.class,

		// The contest-server generate its own RefreshTokens and AccessTokens (hence it acts as its own
		// AuthroizationServer)
		PivotableTokenService.class, })
@Slf4j
@Configuration
public class PivotableWebFluxSpringConfig {

	@Bean
	AdhocSchemaForApi adhocSchemaForApi() {
		return AdhocSchemaForApi.builder().build();
	}

}