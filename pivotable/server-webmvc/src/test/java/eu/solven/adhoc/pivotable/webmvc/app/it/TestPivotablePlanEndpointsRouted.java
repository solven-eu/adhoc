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
package eu.solven.adhoc.pivotable.webmvc.app.it;

import java.io.IOException;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webtestclient.autoconfigure.WebTestClientAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.pivotable.webmvc.app.PivotableServerWebmvcApplication;

/**
 * Integration test that boots the full webmvc application and asserts the
 * {@code /api/v1/cubes/queries/&#123;uuid&#125;/plan/*} routes are *mapped*. Existing unit tests in
 * {@code TestPivotablePlanController} instantiate the controller via {@code new PivotablePlanController(...)} which
 * bypasses Spring's routing — a missing {@code @Import} of the controller is invisible to those tests. This test
 * exercises the routing layer specifically.
 *
 * <p>
 * Assertion shape: the response status must <strong>not</strong> be 404. We don't authenticate, so the JWT chain
 * answers 401 ({@code "Pivotable API Realm"} bearer challenge) — that's the success case here. A 404 would mean Spring
 * has no mapping for the path and is the regression we're protecting against.
 */
@ExtendWith({})
@SpringBootTest(classes = PivotableServerWebmvcApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = { IPivotableSpringProfiles.P_CONFIG_IMPORT })
@ImportAutoConfiguration(exclude = WebTestClientAutoConfiguration.class)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_INMEMORY })
public class TestPivotablePlanEndpointsRouted {

	@LocalServerPort
	int randomServerPort;

	private static final UUID ANY_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

	// Default RestTemplate throws on 4xx/5xx, which would mask the very status codes this test is
	// asserting on. Install a no-op error handler so getForEntity returns the response regardless.
	private final RestTemplate rest = buildRest();

	private static RestTemplate buildRest() {
		RestTemplate template = new RestTemplate();
		template.setErrorHandler(new ResponseErrorHandler() {
			@Override
			public boolean hasError(ClientHttpResponse response) throws IOException {
				return false;
			}
		});
		return template;
	}

	private String url(String path) {
		return "http://localhost:" + randomServerPort + "/api/v1/cubes/queries/" + ANY_UUID + path;
	}

	@Test
	public void testPlanSummaryRouteIsMapped() {
		ResponseEntity<String> response = rest.getForEntity(url("/plan/summary"), String.class);
		Assertions.assertThat(response.getStatusCode())
				.as("/plan/summary must be mapped — 401 means routed but auth-required (good), 404 means controller not registered")
				.isNotEqualTo(HttpStatus.NOT_FOUND);
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
	}

	@Test
	public void testPlanSnapshotRouteIsMapped() {
		ResponseEntity<String> response = rest.getForEntity(url("/plan/snapshot"), String.class);
		Assertions.assertThat(response.getStatusCode()).isNotEqualTo(HttpStatus.NOT_FOUND);
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
	}

	@Test
	public void testPlanChildrenRouteIsMapped() {
		ResponseEntity<String> response = rest.getForEntity(url("/plan/children"), String.class);
		Assertions.assertThat(response.getStatusCode()).isNotEqualTo(HttpStatus.NOT_FOUND);
		Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
	}
}
