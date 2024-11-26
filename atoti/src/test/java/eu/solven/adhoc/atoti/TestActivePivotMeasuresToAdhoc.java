/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.atoti;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import com.qfs.condition.impl.BaseConditions;
import com.quartetfs.fwk.filtering.impl.EqualCondition;
import com.quartetfs.fwk.filtering.impl.FalseCondition;
import com.quartetfs.fwk.filtering.impl.GreaterEqualCondition;
import com.quartetfs.fwk.filtering.impl.InCondition;
import com.quartetfs.fwk.filtering.impl.LowerEqualCondition;
import com.quartetfs.fwk.filtering.impl.OrCondition;
import com.quartetfs.fwk.filtering.impl.TrueCondition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.CopperRegistrations;
import com.activeviam.copper.pivot.pp.LeafIdentityPostProcessor;
import com.activeviam.copper.pivot.pp.LevelFilteringPostProcessor;
import com.activeviam.copper.pivot.pp.StoreLookupPostProcessor;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;

import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.query.GroupByColumns;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Bucketor;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;

public class TestActivePivotMeasuresToAdhoc {

	@BeforeAll
	public static void beforeAll() {
		// make sure that JUL logs are all redirected to SLF4J
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		CopperRegistrations.setupRegistryForTests();
	}

	@Test
	public void testSplitProperties() {
		Properties properties = new Properties();
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).isEmpty();

		properties.setProperty("key", "");
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).isEmpty();

		properties.setProperty("key", "a");
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).containsExactly("a");

		properties.setProperty("key", "a,b");
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");

		properties.setProperty("key", "a,,b");
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");

		properties.setProperty("key", " a\t,\rb  ");
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");
	}

	@Test
	public void testMakePP() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withContributorsCount()
							.withUpdateTimestamp()

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someLevelFilteringMeasure")
							.withPluginKey(LevelFilteringPostProcessor.TYPE)
							.withUnderlyingMeasures("underlying1")
							.withProperty(LevelFilteringPostProcessor.LEVELS_PROPERTY, "level0,level1")
							.withProperty(LevelFilteringPostProcessor.CONDITIONS_PROPERTY, Arrays.asList(new EqualCondition("someValue"), new EqualCondition( 123)))

							// Hidden to check we convert it into an adhoc tag
							.withPostProcessor("someStoreLookupMeasure")
							.withPluginKey(StoreLookupPostProcessor.PLUGIN_KEY)
							.hidden()
							.withProperty(StoreLookupPostProcessor.FIELDS_PROPERTY, "field0,field1")
							.withProperty(StoreLookupPostProcessor.STORE_NAME_PROPERTY, "someStoreName")

							.withPostProcessor("someLeafMeasure")
							.withPluginKey(LeafIdentityPostProcessor.TYPE)
							.withProperty(LeafIdentityPostProcessor.AGGREGATION_FUNCTION, "MAX")
							.withProperty(LeafIdentityPostProcessor.LEAF_LEVELS, "lvl0,lvl1")

				;
				}).withSingleLevelDimension("").build();

		AdhocMeasureBag adhoc = new ActivePivotMeasuresToAdhoc().asBag(cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(6)
				.containsEntry("contributors.COUNT",
						Aggregator.builder().name("contributors.COUNT").aggregationKey("SUM").build())
				.containsEntry("update.TIMESTAMP",
						Aggregator.builder().name("update.TIMESTAMP").aggregationKey("MAX").build())

				.containsEntry("someAggregatedMeasure",
						Aggregator.builder()
								.name("someAggregatedMeasure")
								.columnName("someColumnName")
								.aggregationKey("SUM")
								.build())

				.containsEntry("someLeafMeasure",
						Bucketor.builder()
								.name("someLeafMeasure")
								.aggregationKey("MAX")
								.combinationKey(LeafIdentityPostProcessor.TYPE)
								.groupBy(GroupByColumns.of("lvl1", "lvl0"))
								.build())
				.containsEntry("someLevelFilteringMeasure",
						Filtrator.builder()
								.name("someLevelFilteringMeasure")
								.underlying("underlying1")
								.filter(AndFilter.and(ColumnFilter.isEqualTo("level0", "someValue"),ColumnFilter.isEqualTo("level1", 123)))
								.build())
				.containsEntry("someStoreLookupMeasure",
						Combinator.builder()
								.name("someStoreLookupMeasure")
								.underlying("contributors.COUNT")
								.combinationKey(StoreLookupPostProcessor.PLUGIN_KEY)
								.build());
	}
	@Test
	public void testConditionToFilter_Raw() {
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", "someString")).isEqualTo(ColumnFilter.isEqualTo("someLevel", "someString"));
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", 123)).isEqualTo(ColumnFilter.isEqualTo("someLevel", 123));
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", Arrays.asList("someString", 123))).isEqualTo(ColumnFilter.isIn("someLevel", Arrays.asList("someString", 123)));

		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", null)).isEqualTo(ColumnFilter.builder().column("someLevel").matchNull().build());
	}

	@Test
	public void testConditionToFilter_Conditions() {
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new TrueCondition())).isEqualTo(ColumnFilter.MATCH_ALL);
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new FalseCondition())).isEqualTo(ColumnFilter.MATCH_NONE);

		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new EqualCondition("someString"))).isEqualTo(ColumnFilter.isEqualTo("someLevel", "someString"));
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new InCondition("someString"))).isEqualTo(ColumnFilter.isIn("someLevel", "someString"));
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new InCondition("someString", 123))).isEqualTo(ColumnFilter.isIn("someLevel", Set.of("someString", 123)));

		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", new OrCondition(Arrays.asList(new GreaterEqualCondition(123), new LowerEqualCondition(234))))).isEqualTo(ColumnFilter.builder().column("someLevel").matchIn(Set.of("someString", 123)));
	}


	@Test
	public void testConditionToFilter_DatastoreConditions() {
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", BaseConditions.True())).isEqualTo(ColumnFilter.isEqualTo("someLevel", "TRUE"));
		Assertions.assertThat(ActivePivotMeasuresToAdhoc.convertToAdhoc("someLevel", BaseConditions.False())).isEqualTo(ColumnFilter.isEqualTo("someLevel", "FALSE"));
	}
}
