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
package eu.solven.adhoc.pivotable.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.SumCombination;
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
public class InjectSimpleCubesConfig {

	@Profile(IPivotableSpringProfiles.P_SIMPLE_DATASETS)
	@Bean
	public Void initSimpleCubes(@Qualifier(IPivotableSpringProfiles.P_SELF_ENDPOINT) AdhocSchema schema) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		registerSimple(schema);

		// schemaForApi.registerQuery("delta.EUR", AdhocQuery.builder().measure("delta").andFilter("ccy",
		// "EUR").build());
		// schemaForApi.registerQuery("gamma.USD", AdhocQuery.builder().measure("gamma").andFilter("ccy",
		// "USD").build());
		// schemaForApi.registerQuery("delta+gamma.grandTotal", AdhocQuery.builder().measure("delta+gamma").build());

		return null;
	}

	protected void registerSimple(AdhocSchema schema) {
		InMemoryTable table = prefillInmemoryTable();

		schema.registerTable(table);

		List<IMeasure> measures = new ArrayList<>();

		measures.add(Aggregator.sum("delta"));
		measures.add(Aggregator.sum("gamma"));

		measures.add(Combinator.builder()
				.name("delta+gamma")
				.underlying("delta")
				.underlying("gamma")
				.combinationKey(SumCombination.KEY)
				.build());
		measures.add(Combinator.builder()
				.name("% delta / delta+gamma")
				.underlying("delta")
				.underlying("gamma")
				.combinationKey(ExpressionCombination.KEY)
				.combinationOptions(ImmutableMap.<String, Object>builder()
						.put(ExpressionCombination.KEY_EXPRESSION,
								"IF(delta == null, 0, IF(gamma == null, 1, delta / (delta + gamma)))")
						.build())
				.build());

		schema.registerForest(MeasureForest.fromMeasures("simple", measures));

		schema.registerCube("simple", "simple", "simple");
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
			table.add(ImmutableMap.<String, Object>builder()

					// This is useful to force large tables
					.put("rowIndex", rowIndex.getAndIncrement())

					.put("ccy", country.currencyCode())
					.put("country", country.name())
					.put("capital_city", country.capital())

					.put("gender", faker.gender().binaryTypes())
					.put("city", faker.address().city())

					.put("delta", delta)
					.put("gamma", gamma)

					.build());
		});
		return table;
	}
}
