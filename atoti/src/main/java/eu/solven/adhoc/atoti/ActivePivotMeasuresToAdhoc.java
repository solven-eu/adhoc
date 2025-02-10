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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import com.quartetfs.biz.pivot.definitions.INativeMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.filtering.ICondition;
import com.quartetfs.fwk.filtering.ILogicalCondition;
import com.quartetfs.fwk.filtering.IMatchingCondition;
import com.quartetfs.fwk.filtering.impl.AndCondition;
import com.quartetfs.fwk.filtering.impl.ComparisonMatchingCondition;
import com.quartetfs.fwk.filtering.impl.EqualCondition;
import com.quartetfs.fwk.filtering.impl.FalseCondition;
import com.quartetfs.fwk.filtering.impl.InCondition;
import com.quartetfs.fwk.filtering.impl.OrCondition;
import com.quartetfs.fwk.filtering.impl.TrueCondition;
import com.quartetfs.fwk.types.IExtendedPlugin;
import com.quartetfs.fwk.types.impl.FactoryValue;

import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregator;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.Bucketor;
import eu.solven.adhoc.measure.step.Combinator;
import eu.solven.adhoc.measure.step.Filtrator;
import eu.solven.adhoc.measure.sum.CountAggregator;
import eu.solven.adhoc.measure.sum.SumAggregator;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
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
	public AdhocMeasureBag asBag(String pivotId, IActivePivotDescription desc) {
		AdhocMeasureBag adhocMeasureSet = AdhocMeasureBag.builder().name(pivotId).build();

		// Add natives measures (i.e. ActivePivot measures with a specific aggregation logic)
		desc.getMeasuresDescription().getNativeMeasures().forEach(nativeMeasure -> {
			Aggregator.AggregatorBuilder aggregatorBuilder = convertNativeMeasure(nativeMeasure);

			if (aggregatorBuilder == null) {
				// Happens on unknown/new native measure
				return;
			}

			adhocMeasureSet.addMeasure(aggregatorBuilder.build());
		});
		if (desc.getMeasuresDescription()
				.getNativeMeasures()
				.stream()
				.noneMatch(m -> IMeasureHierarchy.COUNT_ID.equals(m.getName()))) {
			// contributors.COUNT was not present in the description, but it is then added implicitly
			Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder()
					.name(IMeasureHierarchy.COUNT_ID)
					.aggregationKey(CountAggregator.KEY)
					.columnName(CountAggregator.ASTERISK);
			adhocMeasureSet.addMeasure(aggregatorBuilder.build());

			// Do not add update.TIMESTAMP as it is any way ambiguous regarding the underlying columnName
		}

		// Add (pre-)aggregated measures.
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

	protected Aggregator.AggregatorBuilder convertNativeMeasure(INativeMeasureDescription nativeMeasure) {
		String aggregationKey;
		String columnName;
		if (IMeasureHierarchy.COUNT_ID.equals(nativeMeasure.getName())) {
			aggregationKey = CountAggregator.KEY;
			columnName = CountAggregator.ASTERISK;
		} else if (IMeasureHierarchy.TIMESTAMP_ID.equals(nativeMeasure.getName())) {
			aggregationKey = MaxAggregator.KEY;
			// BEWARE There is no standard way to collect update.TIMESTAMP, as many DB does not keep this
			// information
			columnName = "someTimestampColumn";
		} else {
			log.warn("Unsupported native measure: {}", nativeMeasure);
			return null;
		}

		Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder()
				.name(nativeMeasure.getName())
				.aggregationKey(aggregationKey)
				.columnName(columnName);

		transferProperties(nativeMeasure, aggregatorBuilder::tag);
		return aggregatorBuilder;
	}

	protected List<IMeasure> onImplementationClass(IPostProcessorDescription measure,
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

	/**
	 * Generally over-ridden on a per-project basis
	 * 
	 * @param measure
	 * @return
	 */
	protected List<IMeasure> onAdvancedPostProcessor(IPostProcessorDescription measure) {
		log.warn("Measure={} may not be properly converted as it is an advancedPostProcessor", measure.getName());
		return onBasicPostProcessor(measure);
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

		Map<String, Object> combinatorOptions = new LinkedHashMap<>();
		properties.stringPropertyNames()
				.stream()
				// Reject the properties which are implicitly available in Adhoc model
				.filter(k -> !IPostProcessor.UNDERLYING_MEASURES.equals(k))
				.forEach(key -> combinatorOptions.put(key, properties.get(key)));

		combinatorBuilder.combinationOptions(combinatorOptions);

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

		IAdhocFilter filter = makeFilter(measure, properties);

		filtratorBuilder.filter(filter);

		return List.of(filtratorBuilder.build());
	}

	protected IAdhocFilter makeFilter(IPostProcessorDescription measure, Properties properties) {
		List<String> levels = getPropertyList(properties, LevelFilteringPostProcessor.LEVELS_PROPERTY);
		// The conditions property is special, as ActivePivot expect it to be filled with IConditions
		List<?> filters = (List<?>) properties.get(LevelFilteringPostProcessor.CONDITIONS_PROPERTY);

		if (filters.size() != levels.size()) {
			log.warn("There is less filters ({}) than levels ({})", filters.size(), levels.size());

			if (filters.size() < levels.size()) {
				List<Object> addedFilters = new ArrayList<>(filters);

				for (int i = filters.size(); i < levels.size(); i++) {
					log.warn("Adding a fake filter for level={} on measure={}", levels.get(i), measure.getName());
					addedFilters.add(new TrueCondition());
				}

				filters = addedFilters;
			}
		}

		// We do not build explicitly an AND filter, as it may be a sub-optimal filter
		// By chaining AND operations, we may end with a simpler filter (e.g. given a single filter clause)
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;
		for (int i = 0; i < Math.min(levels.size(), filters.size()); i++) {
			IAdhocFilter columnFilter = convertToAdhoc(levels.get(i), filters.get(i));
			filter = AndFilter.and(filter, columnFilter);
		}
		return filter;
	}

	public IAdhocFilter convertToAdhoc(String level, Object rawCondition) {
		if (rawCondition == null) {
			return ColumnFilter.builder().column(level).matchNull().build();
		} else if (rawCondition instanceof com.qfs.condition.ICondition apCondition) {
			log.warn("We should not receive Datastore conditions: {}", apCondition);
			return ColumnFilter.builder().column(level).matchEquals(apCondition.toString()).build();
		} else if (rawCondition instanceof ICondition apCondition) {
			if (rawCondition instanceof IMatchingCondition matchingCondition) {
				if (matchingCondition instanceof EqualCondition apEqualCondition) {
					return ColumnFilter.isEqualTo(level, apEqualCondition.getMatchingParameter());
				} else if (matchingCondition instanceof InCondition apInCondition) {
					Set<?> domain = (Set<?>) apInCondition.getMatchingParameter();
					return ColumnFilter.builder().column(level).matchIn(domain).build();
				} else if (matchingCondition instanceof ComparisonMatchingCondition apComparisonCondition) {
					Object operand = apComparisonCondition.getMatchingParameter();
					boolean greaterThan;
					boolean matchIfEqual;
					// TODO It is unclear how we should matchNull from ActivePivot
					boolean matchIfNull = getMatchIfNull(level, apComparisonCondition);

					switch (apComparisonCondition.getType()) {
					case IMatchingCondition.GREATER:
						greaterThan = true;
						matchIfEqual = false;
						break;
					case IMatchingCondition.LOWER:
						greaterThan = false;
						matchIfEqual = false;
						break;
					case IMatchingCondition.GREATEREQUAL:
						greaterThan = true;
						matchIfEqual = true;
						break;
					case IMatchingCondition.LOWEREQUAL:
						greaterThan = false;
						matchIfEqual = true;
						break;
					default:
						throw new IllegalStateException("Unexpected value: " + apComparisonCondition.getType());
					}
					IValueMatcher valueMatcher = ComparingMatcher.builder()
							.matchIfEqual(matchIfEqual)
							.matchIfNull(matchIfNull)
							.greaterThan(greaterThan)
							.operand(operand)
							.build();
					return ColumnFilter.builder().column(level).valueMatcher(valueMatcher).build();
				} else {
					// ToStringEquals, Like, Comparison
					log.warn("This case is not well handled: {}", matchingCondition);
					return ColumnFilter.builder().column(level).matchEquals(matchingCondition.toString()).build();
				}
			} else if (rawCondition instanceof ILogicalCondition logicalCondition) {
				if (logicalCondition instanceof FalseCondition) {
					return ColumnFilter.MATCH_NONE;
				} else if (logicalCondition instanceof TrueCondition) {
					return ColumnFilter.MATCH_ALL;
				} else if (logicalCondition instanceof OrCondition orCondition) {
					List<IAdhocFilter> subAdhocConditions = Stream.of(orCondition.getConditions())
							.map(subCondition -> convertToAdhoc(level, subCondition))
							.toList();
					return OrFilter.or(subAdhocConditions);
				} else if (logicalCondition instanceof AndCondition andCondition) {
					List<IAdhocFilter> subAdhocConditions = Stream.of(andCondition.getConditions())
							.map(subCondition -> convertToAdhoc(level, subCondition))
							.toList();
					return AndFilter.and(subAdhocConditions);
				} else {
					// Or, And, Not
					log.warn("This case is not well handled: {}", logicalCondition);
					return ColumnFilter.builder().column(level).matchEquals(logicalCondition.toString()).build();
				}
			} else {
				// SubCondition
				log.warn("This case is not well handled: {}", apCondition);
				return ColumnFilter.builder().column(level).matchEquals(apCondition.toString()).build();
			}
		}
		// Assume we received a raw object
		return ColumnFilter.builder().column(level).matching(rawCondition).build();
	}

	protected boolean getMatchIfNull(String level, ComparisonMatchingCondition apComparisonCondition) {
		// BEWARE False on all cases in a first implementation
		return false;
	}

	protected List<IMeasure> onDynamicPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();

		List<String> underlyingNames = getUnderlyingNames(properties);

		List<String> leafLevels = getPropertyList(properties, ABaseDynamicAggregationPostProcessorV2.LEAF_LEVELS);

		Bucketor.BucketorBuilder bucketorBuilder = Bucketor.builder()
				.name(measure.getName())
				.underlyings(underlyingNames)
				.combinationKey(measure.getPluginKey())
				.groupBy(GroupByColumns.named(leafLevels))
				.aggregationKey(properties.getProperty(ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION,
						SumAggregator.KEY));

		Map<String, Object> combinatorOptions = new LinkedHashMap<>();
		properties.stringPropertyNames()
				.stream()
				// Reject the properties which are implicitly available in Adhoc model
				.filter(k -> !IPostProcessor.UNDERLYING_MEASURES.equals(k))
				.filter(k -> !ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION.equals(k))
				// Do not reject leafLevels as they are ordered in ActivePivot, while unordered in Adhoc
				// e.g.: some custom DynamicPP may give leafLevels[0] some particular role
				.forEach(key -> combinatorOptions.put(key, properties.get(key)));

		bucketorBuilder.combinationOptions(combinatorOptions);

		transferProperties(measure, bucketorBuilder::tag);

		return List.of(bucketorBuilder.build());
	}

	public static List<String> getUnderlyingNames(Properties properties) {
		String key = IPostProcessor.UNDERLYING_MEASURES;
		return getPropertyList(properties, key);
	}

	public static List<String> getPropertyList(Properties properties, String key) {
		String propertyAsString = properties.getProperty(key, "").trim();
		return Stream.of(propertyAsString.split(IPostProcessor.SEPARATOR))
				.filter(p -> !Strings.isNullOrEmpty(p))
				// We strip, as these are usually syntactic sugar in XML-files
				.map(String::strip)
				.toList();
	}

	protected void transferProperties(IMeasureMemberDescription apMeasure, Consumer<String> tagConsumer) {
		if (!apMeasure.isVisible()) {
			tagConsumer.accept("hidden");
		}
		if (!Strings.isNullOrEmpty(apMeasure.getGroup())) {
			tagConsumer.accept(apMeasure.getGroup());
		}
	}
}
