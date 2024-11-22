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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
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
import eu.solven.adhoc.api.v1.IAdhocFilter;
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
 * Helps producing `.dot` files, to generate nice graph with GraphViz. This has specific routines to print measure
 * graphs and store references graph.
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

			Aggregator.AggregatorBuilder aggregatorBuilder =
					Aggregator.builder().name(nativeMeasure.getName()).aggregationKey(aggregationKey);

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

		IExtendedPlugin<IPostProcessor<?>> extendedPluginFactory = Registry.getExtendedPlugin(IPostProcessor.class);

		log.info("Known PP keys: {}", extendedPluginFactory.keys());

		desc.getMeasuresDescription().getPostProcessorsDescription().forEach(measure -> {
			List<IMeasure> asMeasures = new ArrayList<>();

			FactoryValue<IPostProcessor<?>> ppFactory = extendedPluginFactory.valueOf(measure.getPluginKey());
			if (ppFactory != null) {
				asMeasures.addAll(onImplementationClass(measure, ppFactory));
			} else {
				// This happens when we did not successfully configured the Registry
				asMeasures.addAll(onAdvancedPostProcessor(measure));
			}

			asMeasures.forEach(adhocMeasureSet::addMeasure);
		});

		return adhocMeasureSet;
	}

	private List<IMeasure> onImplementationClass(IPostProcessorDescription measure,
			FactoryValue<IPostProcessor<?>> ppFactory) {
		log.debug("ppFactory={}", ppFactory);

		Class<?> implementationClass = ppFactory.getImplementationClass();
		if (ABaseDynamicAggregationPostProcessorV2.class.isAssignableFrom(implementationClass)) {
			return onDynamicPostProcessor(measure);
		} else if (AFilteringPostProcessorV2.class.isAssignableFrom(implementationClass)) {
			return onFilteringPostProcessor(measure);
		} else if (ABasicPostProcessorV2.class.isAssignableFrom(implementationClass)) {
			return onBasicPostProcessor(measure);
		} else {
			// This happens on complex AAdvancedPostProcessorV2
			return onAdvancedPostProcessor(measure);
		}
	}

	protected List<IMeasure> onAdvancedPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(properties);

		return List.of(Combinator.builder().name(measure.getName()).underlyings(underlyingNames).build());
	}

	protected List<IMeasure> onBasicPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(properties);

		Combinator.CombinatorBuilder combinatorBuilder =
				Combinator.builder().name(measure.getName()).combinationKey(measure.getPluginKey());
		if (underlyingNames.isEmpty()) {
			// When there is no explicit underlying measure, Atoti relies on contributors.COUNT
			combinatorBuilder.underlying(IMeasureHierarchy.COUNT_ID);
		} else {
			combinatorBuilder.underlyings(underlyingNames);
		}

		return List.of(combinatorBuilder.build());
	}

	protected List<IMeasure> onFilteringPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(properties);

		Filtrator.FiltratorBuilder filtratorBuilder = Filtrator.builder().name(measure.getName());
		if (underlyingNames.isEmpty()) {
			// When there is no explicit underlying measure, Atoti relies on contributors.COUNT
			filtratorBuilder.underlying(IMeasureHierarchy.COUNT_ID);
		} else if (underlyingNames.size() >= 2) {
			log.warn("What's the logic when filtering multiple underlying measures?");
			filtratorBuilder.underlying("Multiple measures? : " + String.join(",", underlyingNames));
		} else {
			filtratorBuilder.underlying(underlyingNames.getFirst());
		}

		String[] levels =
				properties.getProperty(LevelFilteringPostProcessor.LEVELS_PROPERTY, "").split(IPostProcessor.SEPARATOR);
		String[] filters = properties.getProperty(LevelFilteringPostProcessor.CONDITIONS_PROPERTY, "")
				.split(IPostProcessor.SEPARATOR);

		// We do not build explicitly an AND filter, as it may be a sub-optimal filter
		// By chaining AND operations, we may end with a simpler filter (e.g. given a single filter clause)
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
		for (int i = 0; i < Math.min(levels.length, filters.length); i++) {
			filter = AndFilter.and(filter, ColumnFilter.isEqualTo(levels[i], filters[i]));
		}

		filtratorBuilder.filter(filter);

		return List.of(filtratorBuilder.build());
	}

	protected List<IMeasure> onDynamicPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();

		List<String> underlyingNames = getUnderlyingNames(properties);

		List<String> leafLevels = getPropertyList(properties, ABaseDynamicAggregationPostProcessorV2.LEAF_LEVELS);

		Bucketor.BucketorBuilder bucketorBuilder = Bucketor.builder()
				.name(measure.getName())
				.underlyings(underlyingNames)
				.combinationKey(measure.getPluginKey())
				.groupBy(GroupByColumns.of(leafLevels))
				.aggregationKey(properties.getProperty(ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION,
						SumAggregator.KEY));

		transferProperties(measure, bucketorBuilder::tag);

		return List.of(bucketorBuilder.build());
	}

	public static List<String> getUnderlyingNames(Properties properties) {
		String key = IPostProcessor.UNDERLYING_MEASURES;
		return getPropertyList(properties, key);
	}

	public static List<String> getPropertyList(Properties properties, String key) {
		String underlyingMeasures = properties.getProperty(key, "").trim();
		return Stream.of(underlyingMeasures.split(IPostProcessor.SEPARATOR))
				.filter(p -> !Strings.isNullOrEmpty(p))
				.map(String::strip)
				.toList();
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
