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
package eu.solven.adhoc.pivotable.webmvc.app.it;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webtestclient.autoconfigure.WebTestClientAutoConfiguration;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.ColumnStatistics;
import eu.solven.adhoc.beta.schema.TargetedCubeQuery;
import eu.solven.adhoc.dataframe.tabular.IReadableTabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.pivotable.endpoint.AdhocColumnSearch;
import eu.solven.adhoc.pivotable.endpoint.AdhocEndpointSearch;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.TargetedEndpointSchemaMetadata;
import eu.solven.adhoc.pivotable.webmvc.app.PivotableServerWebmvcApplication;
import eu.solven.adhoc.pivotable.webmvc.client.IPivotableServer;
import eu.solven.adhoc.pivotable.webmvc.client.PivotableRestClientServer;
import eu.solven.adhoc.pivotable.webmvc.client.PivotableWebclientServerProperties;
import eu.solven.adhoc.query.cube.CubeQuery;
import lombok.extern.slf4j.Slf4j;

/**
 * Integration test for the webmvc application. Verifies that the full HTTP stack works end-to-end using
 * {@link PivotableRestClientServer} as the blocking client.
 *
 * <p>
 * Counterpart of {@code TestQueryGrandTotalsThroughRouter} in the webflux module.
 *
 * @author Benoit Lacelle
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = PivotableServerWebmvcApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = { IPivotableSpringProfiles.P_CONFIG_IMPORT })
@ImportAutoConfiguration(exclude = WebTestClientAutoConfiguration.class)
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE, IPivotableSpringProfiles.P_INMEMORY })
@TestPropertySource(properties = { "adhoc.pivotable.random.seed=123",
		PivotableWebclientServerProperties.KEY_PLAYER_CONTESTBASEURL + "=http://localhost:LocalServerPort" })
@Slf4j
public class TestQueryGrandTotalsThroughController {

	// https://stackoverflow.com/questions/30312058/spring-boot-how-to-get-the-running-port
	@LocalServerPort
	int randomServerPort;

	@Autowired
	Environment env;

	@Test
	public void testQueryGrandTotal() {
		PivotableWebclientServerProperties properties =
				PivotableWebclientServerProperties.forTests(env, randomServerPort);
		IPivotableServer pivotableServer = PivotableRestClientServer.fromProperties(properties);

		AtomicInteger nbViews = new AtomicInteger();
		IReadableTabularView lastView = null;

		List<PivotableAdhocEndpointMetadata> endpoints = pivotableServer.searchEntrypoints(AdhocEndpointSearch.builder()
				.endpointId(Optional.of(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID))
				.build());

		for (PivotableAdhocEndpointMetadata endpoint : endpoints) {
			log.info("Processing endpoint={}", endpoint);

			AdhocEndpointSearch search =
					AdhocEndpointSearch.builder().endpointId(Optional.of(endpoint.getId())).build();
			List<TargetedEndpointSchemaMetadata> schemas = pivotableServer.searchSchemas(search);

			for (TargetedEndpointSchemaMetadata schema : schemas) {
				Set<String> cubes = schema.getSchema().getCubes().keySet();
				log.info("Considering endpoint={} cubes={}", endpoint, cubes);

				for (String cubeName : cubes) {
					CubeQuery query = CubeQuery.builder().build();
					TargetedCubeQuery targetedQuery = TargetedCubeQuery.builder()
							.endpointId(schema.getEndpoint().getId())
							.query(query)
							.cube(cubeName)
							.build();
					IReadableTabularView view = pivotableServer.executeQuery(targetedQuery);
					log.info("cubeName={} grandTotal={}", cubeName, view);
					nbViews.incrementAndGet();
					lastView = view;
				}
			}
		}

		Assertions.assertThat(nbViews.get()).isGreaterThan(0);

		Assertions.assertThat(MapBasedTabularView.load(lastView).getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of());
	}

	@Test
	public void testQueryColumns() {
		PivotableWebclientServerProperties properties =
				PivotableWebclientServerProperties.forTests(env, randomServerPort);
		IPivotableServer pivotable = PivotableRestClientServer.fromProperties(properties);

		AtomicInteger nbColumns = new AtomicInteger();
		ColumnStatistics lastColumn = null;

		List<PivotableAdhocEndpointMetadata> endpoints = pivotable.searchEntrypoints(AdhocEndpointSearch.builder()
				.endpointId(Optional.of(PivotableAdhocEndpointMetadata.SELF_ENTRYPOINT_ID))
				.build());

		for (PivotableAdhocEndpointMetadata endpoint : endpoints) {
			log.info("Processing endpoint={}", endpoint);

			AdhocEndpointSearch search =
					AdhocEndpointSearch.builder().endpointId(Optional.of(endpoint.getId())).build();
			List<TargetedEndpointSchemaMetadata> schemas = pivotable.searchSchemas(search);

			for (TargetedEndpointSchemaMetadata schema : schemas) {
				// sorted to guarantee a stable ordering of cubes (equivalent to flatMapSequential in the reactor
				// version)
				Set<String> cubes = schema.getSchema().getCubes().keySet();
				log.info("Considering endpoint={} cubes={}", endpoint, cubes);

				for (String cube : cubes.stream().sorted().toList()) {
					log.info("Considering endpoint={} cube={}", endpoint, cube);

					List<ColumnStatistics> columns = pivotable.columnMetadata(AdhocColumnSearch.builder()
							.endpointId(Optional.of(endpoint.getId()))
							.cube(Optional.of(cube))
							.build());

					for (ColumnStatistics column : columns) {
						log.info("cube={} column={} cardinality={}",
								cube,
								column.getColumn(),
								column.getEstimatedCardinality());
						nbColumns.incrementAndGet();
						lastColumn = column;
					}
				}
			}
		}

		Assertions.assertThat(nbColumns.get()).isGreaterThan(0);

		// The last cube in sorted order is `simple` (lowercase sorts after uppercase), matching the reactor version.
		Assertions.assertThat(lastColumn.getHolder()).isEqualTo("simple");
		Assertions.assertThat(lastColumn.getColumn()).isEqualTo("rowIndex");
	}
}
