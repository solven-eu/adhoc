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
import java.util.Set;
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
import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.pivotable.app.PivotableServerApplication;
import eu.solven.adhoc.pivotable.client.IPivotableServer;
import eu.solven.adhoc.pivotable.client.PivotableWebclientServer;
import eu.solven.adhoc.pivotable.client.PivotableWebclientServerProperties;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.query.cube.CubeQuery;
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
	public void testQueryGrandTotal() {
		PivotableWebclientServerProperties properties =
				PivotableWebclientServerProperties.forTests(env, randomServerPort);
		IPivotableServer pivotableServer = PivotableWebclientServer.fromProperties(properties);

		AtomicInteger nbViews = new AtomicInteger();

		ITabularView lastView = pivotableServer
				// Search for self endpoint
				.searchEntrypoints(AdhocEndpointSearch.builder()
						.endpointId(Optional.of(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID))
						.build())
				.flatMap(endpoint -> {
					log.info("Processing endpoint={}", endpoint);

					// Search for the endpoint schema
					AdhocEndpointSearch search =
							AdhocEndpointSearch.builder().endpointId(Optional.of(endpoint.getId())).build();
					return pivotableServer.searchSchemas(search).flatMap(schema -> {
						Set<String> cubes = schema.getSchema().getCubes().keySet();
						log.info("Considering endpoint={} cubes={}", endpoint, cubes);

						return Flux.fromIterable(cubes).flatMap(cubeName -> {
							CubeQuery query = CubeQuery.builder().build();
							TargetedCubeQuery targetedQuery = TargetedCubeQuery.builder()
									.endpointId(schema.getEndpoint().getId())
									.query(query)
									.cube(cubeName)
									.build();
							return pivotableServer.executeQuery(targetedQuery).map(view -> {
								log.info("cubeName={} grandTotal={}", cubeName, view);

								nbViews.incrementAndGet();

								return view;
							});
						});
					});
				})

				.doOnError(t -> {
					throw new IllegalStateException(t);
				})
				// .then()
				.blockLast();

		Assertions.assertThat(nbViews.get()).isGreaterThan(0);

		Assertions.assertThat(MapBasedTabularView.load(lastView).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of());
	}

	@Test
	public void testQueryColumns() {
		PivotableWebclientServerProperties properties =
				PivotableWebclientServerProperties.forTests(env, randomServerPort);
		IPivotableServer pivotable = PivotableWebclientServer.fromProperties(properties);

		AtomicInteger nbColumns = new AtomicInteger();

		ColumnStatistics lastColumn = pivotable
				// Search for self endpoint
				.searchEntrypoints(AdhocEndpointSearch.builder()
						.endpointId(Optional.of(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID))
						.build())
				.flatMap(endpoint -> {
					log.info("Processing endpoint={}", endpoint);

					// Search for the endpoint schema
					AdhocEndpointSearch search =
							AdhocEndpointSearch.builder().endpointId(Optional.of(endpoint.getId())).build();
					return pivotable.searchSchemas(search).flatMap(schema -> {
						Set<String> cubes = schema.getSchema().getCubes().keySet();
						log.info("Considering endpoint={} cubes={}", endpoint, cubes);

						return Flux.fromIterable(cubes).flatMap(cube -> {
							log.info("Considering endpoint={} cube={}", endpoint, cube);

							return pivotable
									.columnMetadata(AdhocColumnSearch.builder()
											.endpointId(Optional.of(endpoint.getId()))
											.cube(Optional.of(cube))
											.build())

									.map(column -> {
										log.info("cube={} column={} cardinality={}",
												cube,
												column.getColumn(),
												column.getEstimatedCardinality());

										nbColumns.incrementAndGet();

										return column;
									});

						});
					});
				})

				.doOnError(t -> {
					throw new IllegalStateException(t);
				})
				// .then()
				.blockLast();

		Assertions.assertThat(nbColumns.get()).isGreaterThan(0);
		// TODO Columns are not processed in the expected order. Hence, the last cube is not always the same. Why?
		Assertions.assertThat(lastColumn.getColumn()).isIn("gamma", "cubeSlicer", "film_rating");
	}
}
