/*
 * Copyright © 2024 Benoit Lacelle (benoit.lacelle@solven.eu) - SOLVEN
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package eu.solven.adhoc.atoti;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.activeviam.copper.pivot.pp.LevelFilteringPostProcessor;
import com.activeviam.pivot.postprocessing.impl.ABaseDynamicAggregationPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.ABasicPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.AFilteringPostProcessorV2;
import com.google.common.base.Strings;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IMeasureMemberDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.types.IExtendedPlugin;
import com.quartetfs.fwk.types.impl.FactoryValue;
import eu.solven.adhoc.aggregations.max.MaxAggregator;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.query.GroupByColumns;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Bucketor;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps producing `.dot` files, to generate nice graph with GraphViz. This has specific routines to print measure graphs and store references graph.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@RequiredArgsConstructor
public class ActivePivotMeasuresToAdhoc {
    public AdhocMeasureBag asBag(IActivePivotDescription desc) {
        AdhocMeasureBag adhocMeasureSet = AdhocMeasureBag.builder().build();

        desc.getMeasuresDescription().getNativeMeasures().forEach(nativeMeasure -> {
            String aggregationKey;
            if (IMeasureHierarchy.COUNT_ID.equals(nativeMeasure.getName())) {
                aggregationKey = SumAggregator.KEY;
            } else if (IMeasureHierarchy.TIMESTAMP_ID.equals(nativeMeasure.getName())) {
                aggregationKey = MaxAggregator.KEY;
            } else {
                log.warn("Unsupported native measure: {}", nativeMeasure);
                return;
            }

            Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder().name(nativeMeasure.getName()).aggregationKey(aggregationKey);

            transferProperties(nativeMeasure, aggregatorBuilder::tag);

            adhocMeasureSet.addMeasure(aggregatorBuilder.build());
        });

        desc.getMeasuresDescription().getAggregatedMeasuresDescription().forEach(preAggregatedMeasure -> {
            Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder()
                .name(preAggregatedMeasure.getName())
                .columnName(preAggregatedMeasure.getFieldName())
                .aggregationKey(preAggregatedMeasure.getPreProcessedAggregation());

            transferProperties(preAggregatedMeasure, aggregatorBuilder::tag);

            adhocMeasureSet.addMeasure(aggregatorBuilder.build());
        });


        IExtendedPlugin<IPostProcessor> extendedPluginFactory = Registry.getExtendedPlugin(IPostProcessor.class);

        log.info("Known PP keys: {}", extendedPluginFactory.keys());

        desc.getMeasuresDescription().getPostProcessorsDescription().forEach(measure -> {
            List<IMeasure> asMeasures = new ArrayList<>();

            FactoryValue<IPostProcessor> ppFactory = extendedPluginFactory.valueOf(measure.getPluginKey());
            if (ppFactory != null) {
                asMeasures.addAll(onImplementationClass(measure, ppFactory ));
            } else {
                // This happens when we did not successfully configured the Registry
                asMeasures.addAll(onAdvancedPostProcessor(measure));
            }

            asMeasures.forEach(adhocMeasureSet::addMeasure);
        });

        return adhocMeasureSet;
    }

    private List<IMeasure> onImplementationClass(IPostProcessorDescription measure, FactoryValue ppFactory) {
        log.debug("ppFactory={}", ppFactory);

        Class<?> implementationClass = ppFactory.getImplementationClass();
        if (ABaseDynamicAggregationPostProcessorV2.class.isAssignableFrom(implementationClass)) {
            return onDynamicPostProcessor(measure);
        } else if (AFilteringPostProcessorV2.class.isAssignableFrom(implementationClass)) {
            return  onFilteringPostProcessor(measure);
        } else if (ABasicPostProcessorV2.class.isAssignableFrom(implementationClass)) {
            return  onBasicPostProcessor(measure);
        } else {
            // This happens on complex AAdvancedPostProcessorV2
            return  onAdvancedPostProcessor(measure);
        }
    }

    protected List<IMeasure> onAdvancedPostProcessor(IPostProcessorDescription measure) {
        Properties properties = measure.getProperties();
        List<String> underlyingNames = getUnderlyingNames(properties);

        return List.of( Combinator.builder().name(measure.getName()).underlyings(underlyingNames).build());
    }

    protected List<IMeasure> onBasicPostProcessor(IPostProcessorDescription measure) {
        Properties properties = measure.getProperties();
        List<String> underlyingNames = getUnderlyingNames(properties);

        Combinator.CombinatorBuilder combinatorBuilder = Combinator.builder().name(measure.getName()).combinationKey(measure.getPluginKey());
        if (underlyingNames.isEmpty()) {
            // When there is no explicit underlying measure, Atoti relies on contributors.COUNT
            combinatorBuilder.underlying(IMeasureHierarchy.COUNT_ID);
        } else {
            combinatorBuilder.underlyings(underlyingNames);
        }

        return List.of(combinatorBuilder.build());
    }

    protected List<IMeasure>  onFilteringPostProcessor(IPostProcessorDescription measure) {
        Properties properties = measure.getProperties();
        List<String> underlyingNames = getUnderlyingNames(properties);

        Filtrator.FiltratorBuilder filtratorBuilder = Filtrator.builder().name(measure.getName());
        if (underlyingNames.isEmpty()) {
            // When there is no explicit underlying measure, Atoti relies on contributors.COUNT
            filtratorBuilder.underlying(IMeasureHierarchy.COUNT_ID);
        } else if (underlyingNames.size() >= 2) {
            log.warn("What's the logic when filtering multiple underlying measures?");
            filtratorBuilder.underlying("Multiple measures? : " + underlyingNames.stream().collect(Collectors.joining(",")));
        } else {
            filtratorBuilder.underlying(underlyingNames.getFirst());
        }

        String[] levels = properties.getProperty(LevelFilteringPostProcessor.LEVELS_PROPERTY, "").split(IPostProcessor.SEPARATOR);
        String[] filters = properties.getProperty(LevelFilteringPostProcessor.CONDITIONS_PROPERTY, "").split(IPostProcessor.SEPARATOR);

        AndFilter.AndFilterBuilder andFilter = AndFilter.builder();
        for (int i = 0 ; i < Math.min(levels.length, filters.length) ; i++) {
            andFilter.filter(ColumnFilter.isEqualTo(levels[i], filters[i]));
        }

        filtratorBuilder.filter(andFilter.build());

        return List.of(filtratorBuilder.build());
    }

    protected List<IMeasure> onDynamicPostProcessor(IPostProcessorDescription measure ) {
        Properties properties = measure.getProperties();

        List<String> underlyingNames = getUnderlyingNames(properties);

        List<String> leafLevels = getPropertyList(properties, ABaseDynamicAggregationPostProcessorV2.LEAF_LEVELS);

        Bucketor.BucketorBuilder bucketorBuilder = Bucketor.builder()
            .name(measure.getName())
            .underlyingNames(underlyingNames)
            .combinatorKey(measure.getPluginKey())
            .groupBy(GroupByColumns.of(leafLevels))
            .aggregationKey(properties.getProperty(ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION, SumAggregator.KEY));

        transferProperties(measure, bucketorBuilder::tag);

        return List.of(bucketorBuilder.build());
    }

    public static List<String> getUnderlyingNames(Properties properties ) {
        String key = IPostProcessor.UNDERLYING_MEASURES;
        return getPropertyList(properties, key);
    }

    public static List<String> getPropertyList(Properties properties, String key) {
        String underlyingMeasures = properties.getProperty(key, "").trim();
        return Stream.of(underlyingMeasures.split(IPostProcessor.SEPARATOR)).filter(p -> !Strings.isNullOrEmpty(p)).map(String::strip).toList();
    }

    protected void transferProperties(IMeasureMemberDescription nativeMeasure, Consumer<String> addTag) {
        if (!nativeMeasure.isVisible()) {
            addTag.accept("hidden");
        }
        if (!Strings.isNullOrEmpty(nativeMeasure.getGroup())) {
            addTag.accept(nativeMeasure.getGroup());
        }
    }
}
