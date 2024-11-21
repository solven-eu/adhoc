package eu.solven.adhoc.atoti;

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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Properties;

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
        IActivePivotInstanceDescription cubeDescription = StartBuilding.cube().withName("someCubeName").withMeasures(measures -> {
            return measures

                    .withContributorsCount().withUpdateTimestamp()

                    .withAggregatedMeasure().sum("someColumnName").withName("someAggregatedMeasure")

                    .withPostProcessor("someLevelFilteringMeasure").withPluginKey(LevelFilteringPostProcessor.TYPE).withUnderlyingMeasures("underlying1").withProperty(LevelFilteringPostProcessor.LEVELS_PROPERTY, "level0,level1").withProperty(LevelFilteringPostProcessor.CONDITIONS_PROPERTY, "someStoreName")

                    // Hidden to check we convert it into an adhoc tag
                    .withPostProcessor("someStoreLookupMeasure").withPluginKey(StoreLookupPostProcessor.PLUGIN_KEY).hidden().withProperty(StoreLookupPostProcessor.FIELDS_PROPERTY, "field0,field1").withProperty(StoreLookupPostProcessor.STORE_NAME_PROPERTY, "someStoreName")

                    .withPostProcessor("someLeafMeasure").withPluginKey(LeafIdentityPostProcessor.TYPE).withProperty(LeafIdentityPostProcessor.AGGREGATION_FUNCTION, "MAX").withProperty(LeafIdentityPostProcessor.LEAF_LEVELS, "lvl0,lvl1")


                    ;
        }).withSingleLevelDimension("").build();

        AdhocMeasureBag adhoc = new ActivePivotMeasuresToAdhoc().asBag(cubeDescription.getActivePivotDescription());

        Assertions.assertThat(adhoc.getNameToMeasure()).hasSize(6)
                .containsEntry("contributors.COUNT", Aggregator.builder().name("contributors.COUNT").aggregationKey("SUM").build())
                .containsEntry("update.TIMESTAMP", Aggregator.builder().name("update.TIMESTAMP").aggregationKey("MAX").build())

                .containsEntry("someAggregatedMeasure", Aggregator.builder().name("someAggregatedMeasure").columnName("someColumnName").aggregationKey("SUM").build())

                .containsEntry("someLeafMeasure", Bucketor.builder().name("someLeafMeasure").aggregationKey("MAX").combinatorKey(LeafIdentityPostProcessor.TYPE).groupBy(GroupByColumns.of("lvl1", "lvl0")).build())
                .containsEntry("someLevelFilteringMeasure", Filtrator.builder().name("someLevelFilteringMeasure").underlying("underlying1").filter(AndFilter.and(ColumnFilter.isEqualTo("level0", "someStoreName"))).build())
                .containsEntry("someLeafMeasure", Combinator.builder().name("someLeafMeasure").underlying("underlying1").build())
                .containsEntry("someStoreLookupMeasure", Combinator.builder().name("someStoreLookupMeasure").underlying("underlying1").build())
        ;
    }
}
