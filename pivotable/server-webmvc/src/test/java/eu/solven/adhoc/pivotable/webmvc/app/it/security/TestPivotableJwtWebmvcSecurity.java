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
package eu.solven.adhoc.pivotable.webmvc.app.it.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.webmvc.security.PivotableJwtWebmvcSecurity;
import lombok.extern.slf4j.Slf4j;

/**
 * Integration tests for the permit-all rules declared in {@link PivotableJwtWebmvcSecurity#configureApi}.
 *
 * <p>
 * Covers the routes that must be publicly accessible without any authentication token, as well as the behaviour of the
 * {@code P_FAKEUSER} conditional permit.
 *
 * @author Benoit Lacelle
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PivotableServerSecurityWebmvcApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.MOCK,
		properties = IPivotableSpringProfiles.P_CONFIG_IMPORT)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE })
@AutoConfigureWebTestClient
@Slf4j
public class TestPivotableJwtWebmvcSecurity {

	@Autowired
	WebTestClient webTestClient;

	/**
	 * The Swagger/OpenAPI docs endpoint ({@code /v3/api-docs}) must be publicly accessible without authentication, as
	 * configured by the permit-all rule in {@link PivotableJwtWebmvcSecurity#configureApi}.
	 */
	@Test
	public void testOpenApiDocs_publicWithoutAuth() {
		webTestClient.get().uri("/v3/api-docs").accept(MediaType.APPLICATION_JSON).exchange().expectStatus().isOk();
	}

	/**
	 * {@code /actuator/health} must be publicly accessible without authentication, as configured by the permit-all rule
	 * in {@link PivotableJwtWebmvcSecurity#configureApi}.
	 */
	@Test
	public void testActuatorHealth_publicWithoutAuth() {
		webTestClient.get()
				.uri("/actuator/health")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()
				.expectStatus()
				.is2xxSuccessful();
	}

	/**
	 * The {@code /api/v1/public/**} prefix is permit-all in {@link PivotableJwtWebmvcSecurity#configureApi}; the
	 * registered {@code /api/v1/public} endpoint must be reachable without authentication.
	 */
	@Test
	public void testApiPublicRoute_accessibleWithoutAuth() {
		webTestClient.get()
				.uri(IPivotableApiConstants.PREFIX + "/public")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()
				.expectStatus()
				.isOk();
	}
}
