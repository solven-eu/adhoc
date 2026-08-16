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
package eu.solven.adhoc.pivotable.app.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.ColumnIdentifier;
import eu.solven.adhoc.beta.schema.CustomMarkerMetadataGenerator;
import eu.solven.adhoc.beta.schema.IAdhocSchema;
import eu.solven.adhoc.beta.schema.IAdhocSchemaRegistrer;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.measure.ThrowingCombination;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.Shiftor;
import eu.solven.adhoc.pivotable.endpoint.PivotableAdhocEndpointMetadata;
import eu.solven.adhoc.pivotable.endpoint.PivotableSchemaRegistry;
import eu.solven.adhoc.table.InMemoryTable;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import net.datafaker.providers.base.Country;

/**
 * Add a simple cube for tests and demo purposes. Requires a `self` schema to be available in Spring
 * {@link ApplicationContext}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.AvoidDuplicateLiterals" })
public class InjectSimpleExampleCubesConfig {

	// `java:S6831` as Sonar states `@Qualifier` is bad on `@Bean`
	@Profile(IPivotableSpringProfiles.P_SIMPLE_DATASETS)
	@Bean
	public Void initSimpleCubes(PivotableSchemaRegistry schemaRegistry) {
		// @Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT)
		IAdhocSchema schema = schemaRegistry.getSchema(PivotableAdhocEndpointMetadata.localhost().getId());

		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerSimple(schema);

		// schemaForApi.registerQuery("delta.EUR", AdhocQuery.builder().measure("delta").andFilter("ccy",
		// "EUR").build());
		// schemaForApi.registerQuery("gamma.USD", AdhocQuery.builder().measure("gamma").andFilter("ccy",
		// "USD").build());
		// schemaForApi.registerQuery("delta+gamma.grandTotal", AdhocQuery.builder().measure("delta+gamma").build());

		return null;
	}

	protected void registerSimple(IAdhocSchema schema) {
		InMemoryTable table = prefillInmemoryTable();

		schema.getRegistrer().registerTable(table);

		List<IMeasure> measures = new ArrayList<>();

		measures.add(Aggregator.sum("delta").toBuilder().tag("δ").build());
		measures.add(Aggregator.sum("gamma").toBuilder().tag("γ").build());
		// `theta` aggregates a sparse column (only ~50% of source rows carry it). Useful for DRILLTHROUGH
		// scenarios where the user wants to see which source rows are missing a column — paired with the
		// `Out of DT` placeholder formatter on the grid side.
		measures.add(Aggregator.sum("theta").toBuilder().tag("θ").build());

		measures.add(Combinator.builder()
				.name("delta+gamma")
				.underlying("delta")
				.underlying("gamma")
				.combinationKey(SumCombination.KEY)
				.tags(Arrays.asList("δ", "γ"))
				.build());
		measures.add(Combinator.builder()
				.name("% delta / (delta+gamma)")
				.underlying("delta")
				.underlying("gamma")
				.combinationKey(EvaluatedExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put(EvaluatedExpressionCombination.K_EXPRESSION,
								"IF(delta == null, 0, IF(gamma == null, 1, delta / (delta + gamma)))")
						.build())
				.tags(Arrays.asList("δ", "γ"))
				.build());

		measures.add(Shiftor.builder()
				.name("delta.FRANCE")
				.underlying("delta")
				.editorKey(SimpleFilterEditor.KEY)
				.editorOptions(Map.of(SimpleFilterEditor.P_SHIFTED, Map.of("country", "France")))
				.tags(Arrays.asList("δ"))
				.build());

		// `delta.FRANCE.Filter` differs from `delta.FRANCE` (Shiftor) in that it wraps the underlying
		// aggregator in a per-aggregator FILTER rather than SHIFTING the cube-side filter. Useful for manual
		// testing of the DRILLTHROUGH path: the Filtrator's FILTER survives into the merged covering query as
		// a CASE WHEN, so non-France rows return NULL for this alias when both `delta` and this measure are
		// queried with DRILLTHROUGH enabled — driving the `Out of DT` placeholder in the grid.
		measures.add(Filtrator.builder()
				.name("delta.FRANCE.Filter")
				.underlying("delta")
				.filter(ColumnFilter.matchEq("country", "France"))
				.tags(Arrays.asList("δ"))
				.build());

		// Always-throwing measure — used by Pivotable e2e / manual tests to exercise the UI's
		// error-management paths (how a failing measure surfaces in the query grid, the navbar,
		// server logs, etc.). Underlying is `delta` purely because a Combinator requires at
		// least one underlying; its value is discarded by ThrowingCombination.
		measures.add(Combinator.builder()
				.name("always_throws")
				.underlying("delta")
				.combinationKey(ThrowingCombination.class.getName())
				.build());

		// Helps testing customMarkers
		measures.add(Combinator.builder()
				.name("ccyFromCustomMarker_Shallow")
				.combinationKey(ReferenceCcyShallowCombination.class.getName())
				.underlying("delta")
				.underlying("gamma")
				.build());
		measures.add(Combinator.builder()
				.name("ccyFromCustomMarker_Deep")
				.combinationKey(ReferenceCcyDeepCombination.class.getName())
				.underlying("delta")
				.underlying("gamma")
				.build());

		IAdhocSchemaRegistrer registrer = schema.getRegistrer();
		registrer.registerForest(MeasureForest.fromMeasures("simple", measures));

		registrer.registerCube("simple", "simple", "simple");
		registrer.tagCube("simple", ImmutableSet.of("inmemory"));

		registrer.registerCustomMarker("ccy",
				EqualsMatcher.matchEq("simple"),
				CustomMarkerMetadataGenerator.builder()
						.path(ReferenceCcyShallowCombination.PATH_SHALLOW_CCY)
						.possibleValues(() -> ImmutableSet.of(ReferenceCcyShallowCombination.CCY_DEFAULT, "USD", "JPY"))
						.defaultValue(() -> Optional.of(ReferenceCcyShallowCombination.CCY_DEFAULT))
						.build());

		registrer.registerCustomMarker("deepCcy",
				EqualsMatcher.matchEq("simple"),
				CustomMarkerMetadataGenerator.builder()
						.path(ReferenceCcyDeepCombination.PATH_DEEP_CCY)
						.possibleValues(() -> ImmutableSet.of(ReferenceCcyDeepCombination.CCY_DEFAULT, "USD", "JPY"))
						.defaultValue(() -> Optional.of(ReferenceCcyDeepCombination.CCY_DEFAULT))
						.build());

		registrer.tagColumn(ColumnIdentifier.builder().isCubeElseTable(true).holder("simple").column("ccy").build(),
				ImmutableSet.of("core"));
	}

	protected InMemoryTable prefillInmemoryTable() {
		InMemoryTable table = InMemoryTable.builder().name("simple").build();

		Random r = new Random(0);
		Faker faker = new Faker(r);

		AtomicLong rowIndex = new AtomicLong();

		IntStream.range(0, 16 * 1024).forEach(index -> {
			double delta = r.nextInt(128) / 100D;
			double gamma = r.nextInt(1024 * 16) / 100D;

			Country country = faker.country();
			ImmutableMap.Builder<String, Object> row = ImmutableMap.<String, Object>builder()

					// This is useful to force large tables
					.put("rowIndex", rowIndex.getAndIncrement())

					.put("ccy", country.currencyCode())
					.put("country", country.name())
					.put("capital_city", country.capital())

					.put("gender", faker.gender().binaryTypes())
					.put("city", faker.address().city())

					.put("delta", delta)
					.put("gamma", gamma);

			// `theta` is intentionally sparse — only ~50% of rows carry it. Lets the DRILLTHROUGH path emit a
			// heterogeneous schema (rows where `theta` is missing → null in the per-aggregator alias), which the
			// SlickGrid `Out of DT` placeholder formatter is built to surface. Exercised by the
			// `localhost8080-drillthrough-out-of-dt.spec.js` Playwright e2e.
			if (r.nextBoolean()) {
				row.put("theta", r.nextInt(2048) / 100D);
			}

			table.add(row.build());
		});
		return table;
	}
}
