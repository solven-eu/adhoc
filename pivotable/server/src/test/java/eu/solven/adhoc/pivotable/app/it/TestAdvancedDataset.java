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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.IAdhocCubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.pivotable.app.PivotableServerApplication;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocSchemaRegistry;
import eu.solven.adhoc.pivotable.endpoint.PivotableEndpointsRegistry;
import eu.solven.adhoc.query.AdhocQuery;
import lombok.extern.slf4j.Slf4j;

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
@ActiveProfiles({ IPivotableSpringProfiles.P_UNSAFE,
		IPivotableSpringProfiles.P_INMEMORY,
		IPivotableSpringProfiles.P_ADVANCED_DATASETS })
@TestPropertySource(properties = { "adhoc.pivotable.random.seed=123" })
@Slf4j
@Disabled("Keep disabled for CI")
public class TestAdvancedDataset {

	@Autowired
	Environment env;

	@Autowired
	PivotableEndpointsRegistry endpointsRegistry;

	@Autowired
	PivotableAdhocSchemaRegistry schemasRegistry;

	@Autowired
	AdhocCubesRegistry cubesRegistry;

	@Test
	public void testDisabled() {
		Assertions.fail("This should be disabled, as they are too slow for CI");
	}

	@Test
	public void testQueryGrandTotal() {
		AtomicInteger nbViews = new AtomicInteger();
		AtomicReference<ITabularView> lastView = new AtomicReference<>();

		endpointsRegistry.getEndpoints().forEach(endpoint -> {
			AdhocSchema schema = schemasRegistry.getSchema(endpoint.getId());

			IAdhocCubeWrapper cube = schema.getNameToCube().get("ban");
			AdhocQuery query = AdhocQuery.builder().build();
			ITabularView view = cube.execute(query);

			nbViews.incrementAndGet();
			lastView.set(view);
		});

		Assertions.assertThat(nbViews.get()).isGreaterThan(0);

		MapBasedTabularView mapBased = MapBasedTabularView.load(lastView.get());

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("count(*)", 26_045_333L));
	}

	@Test
	public void testQueryMetadata() {
		AtomicInteger nbViews = new AtomicInteger();
		// AtomicReference<ITabularView> lastView = new AtomicReference<>();

		endpointsRegistry.getEndpoints().forEach(endpoint -> {
			AdhocSchema schema = schemasRegistry.getSchema(endpoint.getId());

			IAdhocCubeWrapper cube = schema.getNameToCube().get("ban");

			// PivotableCubeMetadata cube2 = cubesRegistry.getCube(PivotableCubeId.of(endpoint.getId(),
			// cube.getName()));

			nbViews.incrementAndGet();

			Assertions.assertThat(cube.getColumns()).hasSize(21);
		});

		Assertions.assertThat(nbViews.get()).isGreaterThan(0);

		// MapBasedTabularView mapBased = MapBasedTabularView.load(lastView.get());
		//
		// Assertions.assertThat(mapBased.getCoordinatesToValues())
		// .hasSize(1)
		// .containsEntry(Map.of(), Map.of("count(*)", 26_045_333L));
	}

}
