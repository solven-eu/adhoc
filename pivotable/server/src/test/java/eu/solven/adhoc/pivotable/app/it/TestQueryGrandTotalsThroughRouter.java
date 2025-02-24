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
package eu.solven.adhoc.pivotable.app.it;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.TargetedAdhocQuery;
import eu.solven.adhoc.pivotable.app.PivotableServerApplication;
import eu.solven.adhoc.pivotable.client.IPivotableServer;
import eu.solven.adhoc.pivotable.client.PivotableWebclientServer;
import eu.solven.adhoc.pivotable.client.PivotableWebclientServerProperties;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointSearch;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

/**
 * This integration-test serves 2 purposes: first it shows how one can chain call to play a game: it can help ensure the
 * API is stable and simple; second, it ensures the API is actually functional (e.g. up to serializibility of involved
 * classes).
 * 
 * @author Benoit Lacelle
 * @see 'TestTSPLifecycle'
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PivotableServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_INMEMORY })
@TestPropertySource(properties = { "adhoc.pivotable.random.seed=123",
		PivotableWebclientServerProperties.KEY_PLAYER_CONTESTBASEURL + "=http://localhost:LocalServerPort" })
@Slf4j
public class TestQueryGrandTotalsThroughRouter {

	// https://stackoverflow.com/questions/30312058/spring-boot-how-to-get-the-running-port
	@LocalServerPort
	int randomServerPort;

	@Autowired
	Environment env;

	@Test
	public void testSinglePlayer() {
		PivotableWebclientServerProperties properties =
				PivotableWebclientServerProperties.forTests(env, randomServerPort);
		IPivotableServer pivotableServer = PivotableWebclientServer.fromProperties(properties);

		AtomicInteger nbViews = new AtomicInteger();

		ITabularView lastView = pivotableServer
				// Search for self entrypoint
				.searchEntrypoints(AdhocEntrypointSearch.builder()
						.entrypointId(Optional.of(AdhocEntrypointMetadata.SELF_ENTRYPOINT_ID))
						.build())
				.flatMap(entrypoint -> {
					log.info("Processing entrypoint={}", entrypoint);

					// Search for the entrypoint schema
					AdhocEntrypointSearch search =
							AdhocEntrypointSearch.builder().entrypointId(Optional.of(entrypoint.getId())).build();
					return pivotableServer.searchSchemas(search).flatMap(schema -> {
						log.info("Considering entrypoint={} cubeNames={}",
								entrypoint,
								schema.getCubeToColumns().keySet());

						return Flux.fromIterable(schema.getCubeToColumns().keySet()).flatMap(cubeName -> {
							AdhocQuery query = AdhocQuery.builder().build();
							return pivotableServer
									.executeQuery(TargetedAdhocQuery.builder().query(query).cube(cubeName).build())
									.map(view -> {
										log.info("cubeName={} grandTotal={}", cubeName, view);

										nbViews.incrementAndGet();

										return view;
									});
						});
					});
				})

				.doOnError(t -> {
					log.error("Something went wrong", t);
				})
				// .then()
				.blockLast();

		Assertions.assertThat(nbViews.get()).isGreaterThan(0);

		MapBasedTabularView mapBased = MapBasedTabularView.load(lastView);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("count(*)", 2));
	}
}
