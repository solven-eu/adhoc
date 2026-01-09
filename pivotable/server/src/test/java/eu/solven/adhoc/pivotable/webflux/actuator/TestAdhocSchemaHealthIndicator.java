package eu.solven.adhoc.pivotable.webflux.actuator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.mock.env.MockEnvironment;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.InMemoryTable;

public class TestAdhocSchemaHealthIndicator {
	AdhocSchema schema = AdhocSchema.builder().env(new MockEnvironment()).build();

	@Test
	public void testEmpty() {
		AdhocSchemaHealthIndicator indicator = new AdhocSchemaHealthIndicator(schema);

		ReactiveHealthIndicator contributor = indicator.getContributor("notExisting");
		Health health = contributor.getHealth(false).block();
		Assertions.assertThat(health.getStatus()).isEqualTo(Status.UNKNOWN);
	}

	@Test
	public void testOne() {
		ITableWrapper table = InMemoryTable.builder().build();
		CubeWrapper cube =
				CubeWrapper.builder().name("someCubeName").table(table).forest(MeasureForest.empty()).build();
		schema.registerCube(cube);

		AdhocSchemaHealthIndicator indicator = new AdhocSchemaHealthIndicator(schema);

		ReactiveHealthIndicator contributor = indicator.getContributor(cube.getName());
		Health health = contributor.getHealth(true).block();
		Assertions.assertThat(health.getStatus()).isEqualTo(Status.UP);
		Assertions.assertThat(health.getDetails())
				.containsEntry("columns", 0)
				.containsEntry("measures", 0)
				.containsEntry("table", ImmutableMap.of("name", "inMemory", "rows", 0))
				.hasSize(3);
	}
}
