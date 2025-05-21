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
package eu.solven.adhoc.atoti.migration;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.CopperRegistrations;
import com.activeviam.copper.pivot.pp.DrillupPostProcessor;
import com.activeviam.copper.pivot.pp.LeafIdentityPostProcessor;
import com.activeviam.copper.pivot.pp.LevelFilteringPostProcessor;
import com.activeviam.copper.pivot.pp.ShiftPostProcessor;
import com.activeviam.copper.pivot.pp.StoreLookupPostProcessor;
import com.activeviam.pivot.postprocessing.impl.ADynamicAggregationPostProcessorV2;
import com.google.common.collect.ImmutableMap;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.impl.PostProcessorDescription;
import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;
import com.quartetfs.fwk.filtering.impl.EqualCondition;

import eu.solven.adhoc.atoti.custom.CustomActivePivotMeasureToAdhoc;
import eu.solven.adhoc.atoti.custom.CustomAtotiConditionCubeToAdhoc;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.*;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestAtotiMeasureToAdhoc {

	final AtotiMeasureToAdhoc apMeasuresToAdhoc = AtotiMeasureToAdhoc.builder().build();

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
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).isEmpty();

		properties.setProperty("key", "");
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).isEmpty();

		properties.setProperty("key", "a");
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).containsExactly("a");

		properties.setProperty("key", "a,b");
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");

		properties.setProperty("key", "a,,b");
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");

		properties.setProperty("key", " a\t,\rb  ");
		Assertions.assertThat(AtotiMeasureToAdhoc.getPropertyList(properties, "key")).containsExactly("a", "b");
	}

	@Test
	public void testNativeImplicit() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(1)
				.containsEntry("contributors.COUNT",
						Aggregator.builder()
								.name("contributors.COUNT")
								.aggregationKey("COUNT")
								.columnName("*")
								.build());
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

				;
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsEntry("contributors.COUNT",
						Aggregator.builder().name("contributors.COUNT").aggregationKey("COUNT").columnName("*").build())
				.containsEntry("update.TIMESTAMP",
						Aggregator.builder()
								.name("update.TIMESTAMP")
								.aggregationKey("MAX")
								.columnName("someTimestampColumn")
								.build())

				.containsEntry("someAggregatedMeasure",
						Aggregator.builder()
								.name("someAggregatedMeasure")
								.columnName("someColumnName")
								.aggregationKey("SUM")
								.build())

		;
	}

	@Test
	public void testMakePP_levelFiltering() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someLevelFilteringMeasure")
							.withPluginKey(LevelFilteringPostProcessor.TYPE)
							.withUnderlyingMeasures("someAggregatedMeasure")
							.withProperty(LevelFilteringPostProcessor.LEVELS_PROPERTY, "level0,level1")
							.withProperty(LevelFilteringPostProcessor.CONDITIONS_PROPERTY,
									Arrays.asList(new EqualCondition("someValue"), new EqualCondition(123)));
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someLevelFilteringMeasure",
						Filtrator.builder()
								.name("someLevelFilteringMeasure")
								.underlying("someAggregatedMeasure")
								.filter(AndFilter.and(ColumnFilter.isEqualTo("level0", "someValue"),
										ColumnFilter.isEqualTo("level1", 123)))
								.build());
	}

	@Test
	public void testMakePP_storeLookup() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							// Hidden to check we convert it into an adhoc tag
							.withPostProcessor("someStoreLookupMeasure")
							.withPluginKey(StoreLookupPostProcessor.PLUGIN_KEY)
							.withUnderlyingMeasures("someAggregatedMeasure")
							.withProperty(StoreLookupPostProcessor.FIELDS_PROPERTY, "field0,field1")
							.withProperty(StoreLookupPostProcessor.STORE_NAME_PROPERTY, "someStoreName");
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someStoreLookupMeasure",
						Combinator.builder()
								.name("someStoreLookupMeasure")
								.underlying("someAggregatedMeasure")
								.combinationKey(StoreLookupPostProcessor.PLUGIN_KEY)
								.combinationOptions(ImmutableMap.<String, Object>builder()
										.put("storeName", "someStoreName")
										.put("fields", "field0,field1")
										.build())
								.build());
	}

	@Test
	public void testMakePP_dynamicAggregation() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someLeafMeasure")
							.withPluginKey(LeafIdentityPostProcessor.TYPE)
							.withUnderlyingMeasures("someAggregatedMeasure")
							.withProperty(LeafIdentityPostProcessor.AGGREGATION_FUNCTION, "MAX")
							.withProperty(LeafIdentityPostProcessor.LEAF_LEVELS, "lvl0,lvl1")
							.withProperty("customKey", "customValue");
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someLeafMeasure",
						Bucketor.builder()
								.name("someLeafMeasure")
								.aggregationKey("MAX")
								.underlying("someAggregatedMeasure")
								.combinationKey(LeafIdentityPostProcessor.TYPE)
								.combinationOptions(ImmutableMap.<String, Object>builder()
										// leafLevels keep the original ordering
										.put("customKey", "customValue")
										.put(ADynamicAggregationPostProcessorV2.LEAF_LEVELS, "lvl0,lvl1")
										.build())
								// groupBy may be re-ordered
								.groupBy(GroupByColumns.named("lvl1", "lvl0"))
								.build());
	}

	@Test
	public void testMakePP_DrillUp() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someDrilledUpMeasure")
							.withPluginKey(DrillupPostProcessor.PLUGIN_KEY)
							.withUnderlyingMeasures("someAggregatedMeasure")
							.withProperty(DrillupPostProcessor.PARENT_HIERARCHIES, "level1,level2")
							.withProperty("customKey", "customValue");
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someDrilledUpMeasure",
						Unfiltrator.builder()
								.name("someDrilledUpMeasure")
								.underlying("someAggregatedMeasure")
								.column("level1")
								.column("level2")
								.build());
	}

	@Test
	public void testMakePP_shift() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someShiftedMeasure")
							.withPluginKey(ShiftPostProcessor.TYPE)
							.withUnderlyingMeasures("someAggregatedMeasure")
							.withProperty("customKey", "customValue");
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someShiftedMeasure",
						Shiftor.builder()
								.name("someShiftedMeasure")
								.editorKey(ShiftPostProcessor.TYPE)
								.editorOptions(
										ImmutableMap.<String, Object>builder().put("customKey", "customValue").build())
								.underlying("someAggregatedMeasure")
								.build());
	}

	@Test
	public void testMakePP_arithmeticFormula() {
		IActivePivotInstanceDescription cubeDescription =
				StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
					return measures

							.withAggregatedMeasure()
							.sum("someColumnName")
							.withName("someAggregatedMeasure")

							.withPostProcessor("someFormula")
							.withPluginKey(ArithmeticFormulaPostProcessor.PLUGIN_KEY)
							.withProperty(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
									"aggregatedValue[someAggregatedMeasure],double[10000],*")
							.withProperty("customKey", "customValue");
				}).withSingleLevelDimension("someL").build();

		IMeasureForest adhoc =
				apMeasuresToAdhoc.asForest(cubeDescription.getId(), cubeDescription.getActivePivotDescription());

		Assertions.assertThat(adhoc.getNameToMeasure())
				.hasSize(3)
				.containsKeys("contributors.COUNT", "someAggregatedMeasure")

				.containsEntry("someFormula",
						Combinator.builder()
								.name("someFormula")
								.combinationKey(ArithmeticFormulaPostProcessor.PLUGIN_KEY)
								.combinationOptions(ImmutableMap.<String, Object>builder()
										.put("customKey", "customValue")
										.put(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
												"aggregatedValue[someAggregatedMeasure],double[10000],*")
										.build())
								.underlying("someAggregatedMeasure")
								.build());
	}

	@Test
	public void testCustomConditions() {
		CustomActivePivotMeasureToAdhoc converter = CustomActivePivotMeasureToAdhoc.customBuilder().build();

		Assertions.assertThat(converter.getApConditionToAdhoc()).isInstanceOf(CustomAtotiConditionCubeToAdhoc.class);
	}

	@Test
	public void testOnColumnator() {
		CustomActivePivotMeasureToAdhoc converter = CustomActivePivotMeasureToAdhoc.customBuilder().build();

		IPostProcessorDescription pp = new PostProcessorDescription();
		pp.setName("someMeasureName");
		pp.setUnderlyingMeasures("m1,m2");
		pp.setPluginKey("somePluginKey");

		Properties properties = new Properties();
		pp.setProperties(properties);

		Columnator columnator = (Columnator) converter.onColumnator(pp, b -> {
			b.columns(Set.of("l1", "l2"));
		});

		Assertions.assertThat(columnator.getName()).isEqualTo("someMeasureName");
		Assertions.assertThat(columnator.getCombinationKey()).isEqualTo("somePluginKey");
		Assertions.assertThat(columnator.getColumns()).isEqualTo(Set.of("l1", "l2"));
	}
}
