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
package eu.solven.adhoc.atoti.translation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.activeviam.copper.pivot.pp.DrillupPostProcessor;
import com.activeviam.copper.pivot.pp.LevelFilteringPostProcessor;
import com.activeviam.pivot.postprocessing.impl.AAdvancedPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.ABaseDynamicAggregationPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.ABasicPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.ADynamicAggregationPostProcessorV2;
import com.activeviam.pivot.postprocessing.impl.AFilteringPostProcessorV2;
import com.google.common.base.Strings;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IAggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IMeasureMemberDescription;
import com.quartetfs.biz.pivot.definitions.INativeMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.impl.JoinMeasureDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ABaseDynamicAggregationPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ABasicPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.AFilteringPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ALocationShiftPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.filtering.impl.TrueCondition;
import com.quartetfs.fwk.types.IExtendedPlugin;
import com.quartetfs.fwk.types.impl.FactoryValue;

import eu.solven.adhoc.atoti.table.AtotiTranscoder;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.combination.EvaluatedExpressionCombination;
import eu.solven.adhoc.measure.combination.ReversePolishCombination;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps producing `.dot` files, to generate nice graph with GraphViz. This has specific routines to print measure
 * graphs and store references graph.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder
// Add constructor to facilitate custom overloads
@AllArgsConstructor
@SuppressWarnings("PMD.GodClass")
@Getter(AccessLevel.PROTECTED)
public class AtotiMeasureToAdhoc {
	/**
	 * Hints the target table model queried by Adhoc measures as migrated from ActivePivot.
	 * 
	 * @author Benoit Lacelle
	 */
	@SuppressWarnings("PMD.FieldNamingConventions")
	public enum SourceMode {
		/**
		 * Adhoc will query data equivalent to the data in Atoti Datastore.
		 */
		Datastore,
		/**
		 * Adhoc will query data equivalent to the data in Atoti Cube.
		 */
		Cube,
	}

	@Builder.Default
	@NonNull
	final AtotiConditionCubeToAdhoc apConditionToAdhoc = new AtotiConditionCubeToAdhoc();

	@Builder.Default
	@NonNull
	final ITableTranscoder transcoder = AtotiTranscoder.builder().build();

	@NonNull
	SourceMode sourceMode;

	public IMeasureForest asForest(String pivotId, IActivePivotDescription desc) {
		MeasureForestBuilder measureForest = MeasureForest.builder().name(pivotId);

		// Add natives measures (i.e. ActivePivot measures with a specific aggregation logic)
		desc.getMeasuresDescription().getNativeMeasures().forEach(nativeMeasure -> {
			Aggregator.AggregatorBuilder aggregatorBuilder = convertNativeMeasure(nativeMeasure);

			if (aggregatorBuilder == null) {
				// Happens on unknown/new native measure
				return;
			}

			measureForest.measure(aggregatorBuilder.build());
		});
		if (desc.getMeasuresDescription()
				.getNativeMeasures()
				.stream()
				.noneMatch(m -> IMeasureHierarchy.COUNT_ID.equals(m.getName()))) {
			// contributors.COUNT was not present in the description, but it is then added implicitly
			Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder()
					.name(IMeasureHierarchy.COUNT_ID)
					.aggregationKey(CountAggregation.KEY)
					.columnName(ICountMeasuresConstants.ASTERISK);
			measureForest.measure(aggregatorBuilder.build());

			// Do not add update.TIMESTAMP as it is any way ambiguous regarding the underlying columnName
		}

		// Add (pre-)aggregated measures.
		desc.getMeasuresDescription().getAggregatedMeasuresDescription().forEach(preAggregatedMeasure -> {
			measureForest.measures(onAggregatedMeasure(preAggregatedMeasure));
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

			asMeasures.forEach(measureForest::measure);
		});

		return measureForest.build();
	}

	protected List<String> getUnderlyingNames(IPostProcessorDescription measure) {
		return getUnderlyingNames(measure.getProperties());
	}

	public static List<String> getUnderlyingNames(Properties properties) {
		String key = IPostProcessor.UNDERLYING_MEASURES;
		return getPropertyList(properties, key);
	}

	public static List<String> getPropertyList(Properties properties, String key) {
		String propertyAsString = properties.getProperty(key, "").trim();
		return Stream.of(propertyAsString.split(IPostProcessor.SEPARATOR))
				.filter(p -> !Strings.isNullOrEmpty(p))
				// We strip, as these whitespaces are usually syntactic sugar in XML-files
				.map(String::strip)
				.toList();
	}

	protected void transferTagProperties(IMeasureMemberDescription measure, Consumer<String> tagConsumer) {
		if (!measure.isVisible()) {
			tagConsumer.accept("hidden");
		}
		if (!Strings.isNullOrEmpty(measure.getGroup())) {
			tagConsumer.accept("group=" + measure.getGroup());
		}
		String folder = measure.getFolder();
		if (!Strings.isNullOrEmpty(folder)) {
			tagConsumer.accept("folder=" + folder);
		}
	}

	protected List<IMeasure> onAggregatedMeasure(IAggregatedMeasureDescription preAggregatedMeasure) {
		Aggregator.AggregatorBuilder aggregatorBuilder = Aggregator.builder()
				.name(preAggregatedMeasure.getName())
				.aggregationKey(preAggregatedMeasure.getPreProcessedAggregation());

		if (sourceMode == SourceMode.Datastore) {
			String rawFieldName = preAggregatedMeasure.getFieldName();

			String columnName;
			if (preAggregatedMeasure instanceof JoinMeasureDescription) {
				// BEWARE This case is probably buggy
				columnName = rawFieldName;
			} else {
				columnName = quoteIfNecessary(rawFieldName);
			}

			// BEWARE We do not quote the field as we expect a measure containing a `.` (e.g. )
			aggregatorBuilder.columnName(columnName);
		} else if (sourceMode == SourceMode.Cube) {
			String rawMeasureName = preAggregatedMeasure.getName();

			// Typical measure name is `k1.SUM`. The `.` would be interpreted as a `JOIN` in
			// `JooqTableQueryFactory.name(String)`.
			String columnName = quoteIfNecessary(rawMeasureName);

			// Do not query the underlying field name, as we expect to export the Cube columns
			aggregatorBuilder.columnName(columnName);
		}

		transferTagProperties(preAggregatedMeasure, aggregatorBuilder::tag);

		return List.of(aggregatorBuilder.build());
	}

	private String quoteIfNecessary(String rawFieldName) {
		String columnName;
		if (rawFieldName.contains(".")) {
			columnName = '\"' + rawFieldName + '\"';
		} else {
			columnName = rawFieldName;
		}
		return columnName;
	}

	protected Aggregator.AggregatorBuilder convertNativeMeasure(INativeMeasureDescription nativeMeasure) {
		String aggregationKey;
		String columnName;
		if (IMeasureHierarchy.COUNT_ID.equals(nativeMeasure.getName())) {
			aggregationKey = CountAggregation.KEY;
			columnName = ICountMeasuresConstants.ASTERISK;
		} else if (IMeasureHierarchy.TIMESTAMP_ID.equals(nativeMeasure.getName())) {
			aggregationKey = MaxAggregation.KEY;
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

		transferTagProperties(nativeMeasure, aggregatorBuilder::tag);
		return aggregatorBuilder;
	}

	protected List<IMeasure> onImplementationClass(IPostProcessorDescription measure,
			FactoryValue<IPostProcessor<?>> ppFactory) {
		log.debug("ppFactory={}", ppFactory);

		Class<?> implementationClass = ppFactory.getImplementationClass();
		if (ABaseDynamicAggregationPostProcessorV2.class.isAssignableFrom(implementationClass)
				|| ABaseDynamicAggregationPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onDynamicPostProcessor(measure, builder -> {
				// no default customization
			});
		} else if (AFilteringPostProcessorV2.class.isAssignableFrom(implementationClass)
				|| AFilteringPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onFilteringPostProcessor(measure, builder -> {
				// no default customization
			});
		} else if (DrillupPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onDrillupPostProcessor(measure);
		} else if (ALocationShiftPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onLocationShiftPosProcessor(measure, builder -> {

			});
		} else if (ABasicPostProcessorV2.class.isAssignableFrom(implementationClass)
				|| ABasicPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onBasicPostProcessor(measure);
		} else if (ArithmeticFormulaPostProcessor.class.isAssignableFrom(implementationClass)) {
			return onArithmeticFormulaPostProcessor(measure);
		} else {
			// This happens on complex AAdvancedPostProcessorV2
			return onAdvancedPostProcessor(measure);
		}
	}

	/**
	 * Generally overridden on a per-project basis
	 * 
	 * @param measure
	 * @return
	 */
	protected List<IMeasure> onAdvancedPostProcessor(IPostProcessorDescription measure) {
		log.warn("Measure={} may not be properly converted as {} is an advancedPostProcessor",
				measure.getName(),
				measure.getPluginKey());

		// Dispatchor is not most probable transformator for an advancedPostProcessor
		return onDispatchor(measure, b -> {
			// We have no information about the Dispatchor: we prefer to return as-is the underlying measure
			// Else SUM on complex objects would generate Strings
			b.aggregationKey(CoalesceAggregation.KEY);
		});
	}

	/**
	 * @return the default measure when no measure is expressed.
	 */
	protected String getDefaultMeasure() {
		return IMeasureHierarchy.COUNT_ID;
	}

	protected List<IMeasure> onBasicPostProcessor(IPostProcessorDescription measure) {
		return onCombinator(measure, getUnderlyingNames(measure), b -> {

		}, Function.identity());
	}

	protected List<IMeasure> onCombinator(IPostProcessorDescription measure,
			List<String> underlyingNames,
			Consumer<Combinator.CombinatorBuilder> onBuilder,
			Function<Map<String, Object>, Map<String, Object>> onOptions) {
		Combinator.CombinatorBuilder combinatorBuilder = Combinator.builder().name(measure.getName());
		transferTagProperties(measure, combinatorBuilder::tag);

		if (underlyingNames.isEmpty()) {
			// When there is no explicit underlying measure, Atoti relies on contributors.COUNT
			combinatorBuilder.underlying(getDefaultMeasure());
		} else {
			combinatorBuilder.underlyings(underlyingNames);
		}

		Map<String, Object> combinatorOptions = propertiesToOptions(measure.getProperties());

		combinatorBuilder.combinationKey(measure.getPluginKey());
		combinatorBuilder.combinationOptions(onOptions.apply(combinatorOptions));

		onBuilder.accept(combinatorBuilder);

		return List.of(combinatorBuilder.build());
	}

	/**
	 * Typically used to convert from `l@h@d` to `l`.
	 * 
	 * @param level
	 * @return
	 */
	protected String levelToColumn(String level) {
		return transcoder.underlyingNonNull(level);
	}

	protected List<IMeasure> onFilteringPostProcessor(IPostProcessorDescription measure,
			Consumer<Filtrator.FiltratorBuilder> onBuilder) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(measure);

		Filtrator.FiltratorBuilder filtratorBuilder = Filtrator.builder().name(measure.getName());
		transferTagProperties(measure, filtratorBuilder::tag);
		filtratorBuilder.underlying(getSingleUnderylingMeasure(underlyingNames));

		IAdhocFilter filter = makeFilter(measure, properties);

		filtratorBuilder.filter(filter);

		onBuilder.accept(filtratorBuilder);

		return List.of(filtratorBuilder.build());
	}

	// TODO ParentValuePostProcessor is not managed as it requires multi-level hierarchies
	protected List<IMeasure> onLocationShiftPosProcessor(IPostProcessorDescription measure,
			Consumer<Shiftor.ShiftorBuilder> onBuilder) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(measure);

		Shiftor.ShiftorBuilder shiftorBuilder = Shiftor.builder().name(measure.getName());
		transferTagProperties(measure, shiftorBuilder::tag);
		shiftorBuilder.underlying(getSingleUnderylingMeasure(underlyingNames));

		shiftorBuilder.editorKey(measure.getPluginKey());

		Map<String, Object> editorOptions = propertiesToOptions(properties);
		shiftorBuilder.editorOptions(editorOptions);

		onBuilder.accept(shiftorBuilder);

		return List.of(shiftorBuilder.build());
	}

	protected String getSingleUnderylingMeasure(List<String> underlyingNames) {
		if (underlyingNames.isEmpty()) {
			// When there is no explicit underlying measure, Atoti relies on contributors.COUNT
			return getDefaultMeasure();
		} else if (underlyingNames.size() >= 2) {
			log.warn("What's the logic when filtering multiple underlying measures?");
			return "Multiple measures? : " + String.join(",", underlyingNames);
		} else {
			return underlyingNames.getFirst();
		}
	}

	protected List<IMeasure> onDrillupPostProcessor(IPostProcessorDescription measure) {
		Properties properties = measure.getProperties();
		List<String> underlyingNames = getUnderlyingNames(measure);

		Unfiltrator.UnfiltratorBuilder unfiltratorBuilder = Unfiltrator.builder().name(measure.getName());
		transferTagProperties(measure, unfiltratorBuilder::tag);

		unfiltratorBuilder.underlying(getSingleUnderylingMeasure(underlyingNames));

		List<String> parentHierarchies = getPropertyList(properties, DrillupPostProcessor.PARENT_HIERARCHIES);
		unfiltratorBuilder.columns(parentHierarchies.stream().map(this::levelToColumn).toList());

		return List.of(unfiltratorBuilder.build());
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
			IAdhocFilter columnFilter = apConditionToAdhoc.convertToAdhoc(levelToColumn(levels.get(i)), filters.get(i));
			filter = AndFilter.and(filter, columnFilter);
		}
		return filter;
	}

	protected List<IMeasure> onDynamicPostProcessor(IPostProcessorDescription measure,
			Consumer<Partitionor.PartitionorBuilder> onBuilder) {
		Properties properties = measure.getProperties();

		List<String> underlyingNames = getUnderlyingNames(measure);

		List<String> leafLevels = getPropertyList(properties, ABaseDynamicAggregationPostProcessorV2.LEAF_LEVELS);

		Partitionor.PartitionorBuilder partitionorBuilder = Partitionor.builder()
				.name(measure.getName())
				.underlyings(underlyingNames)
				.combinationKey(measure.getPluginKey())
				.groupBy(makeGroupBy(leafLevels))
				.aggregationKey(properties.getProperty(ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION,
						SumAggregation.KEY));

		transferTagProperties(measure, partitionorBuilder::tag);

		Map<String, Object> combinatorOptions =
				propertiesToOptions(properties, ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION);

		partitionorBuilder.combinationOptions(combinatorOptions);

		onBuilder.accept(partitionorBuilder);

		return List.of(partitionorBuilder.build());
	}

	protected IAdhocGroupBy makeGroupBy(List<String> leafLevels) {
		return GroupByColumns.named(leafLevels.stream().map(this::levelToColumn).toList());
	}

	// TODO Some formula may be more complex than a simple Combinator
	protected List<IMeasure> onArithmeticFormulaPostProcessor(IPostProcessorDescription measure) {
		String formula = measure.getProperties().getProperty(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY);
		String notation = formula.replaceAll(Pattern.quote("aggregatedValue["),
				Matcher.quoteReplacement(EvaluatedExpressionCombination.P_UNDERLYINGS + "["));

		List<String> underlyingNames = List.copyOf(ReversePolishCombination.parseUnderlyingMeasures(notation));

		List<IMeasure> combinator = onCombinator(measure, underlyingNames, b -> {
			b.combinationKey(ReversePolishCombination.class.getName());
		}, options -> {
			options.remove(ArithmeticFormulaPostProcessor.FORMULA_PROPERTY);
			options.put(ReversePolishCombination.K_NOTATION, notation);

			return options;
		});
		return combinator;
	}

	protected IMeasure onColumnator(IPostProcessorDescription measure,
			Consumer<Columnator.ColumnatorBuilder> builderConsumer) {
		Columnator.ColumnatorBuilder columnatorBuilder = Columnator.builder().name(measure.getName());
		transferTagProperties(measure, columnatorBuilder::tag);

		columnatorBuilder.underlyings(getUnderlyingNames(measure));

		Map<String, Object> columnatorOptions = new LinkedHashMap<>();

		Properties properties = measure.getProperties();
		properties.stringPropertyNames()
				.stream()
				// Reject the properties which are implicitly available in Adhoc model
				.filter(k -> !IPostProcessor.UNDERLYING_MEASURES.equals(k))
				.forEach(key -> columnatorOptions.put(key, properties.get(key)));

		columnatorBuilder.combinationKey(measure.getPluginKey());
		columnatorBuilder.combinationOptions(columnatorOptions);

		builderConsumer.accept(columnatorBuilder);

		return columnatorBuilder.build();
	}

	/**
	 *
	 * @param measure
	 *            ActivePivot measure
	 * @param dispatchorBuilderConsumer
	 *            consumer for the main DispatchorBuilder
	 * @return
	 */
	protected List<IMeasure> onDispatchor(IPostProcessorDescription measure,
			Consumer<Dispatchor.DispatchorBuilder> dispatchorBuilderConsumer) {
		Properties properties = measure.getProperties();

		List<String> underlyingNames = getUnderlyingNames(properties);
		if (underlyingNames.size() != 1) {
			throw new IllegalArgumentException(
					"[%s] Expected a single underlying but received %s".formatted(measure.getName(), underlyingNames));
		}

		Dispatchor.DispatchorBuilder dispatchorBuilder = Dispatchor.builder().name(measure.getName());

		dispatchorBuilder.underlying(underlyingNames.getFirst());
		{
			Map<String, Object> decompositionOptions = propertiesToOptions(properties);

			dispatchorBuilder.decompositionKey(measure.getPluginKey()).decompositionOptions(decompositionOptions);
		}
		dispatchorBuilder.aggregationKey(properties
				.getProperty(ABaseDynamicAggregationPostProcessorV2.AGGREGATION_FUNCTION, SumAggregation.KEY));

		transferTagProperties(measure, dispatchorBuilder::tag);

		dispatchorBuilderConsumer.accept(dispatchorBuilder);

		return List.of(dispatchorBuilder.build());
	}

	protected Map<String, Object> propertiesToOptions(Properties properties, String... excludedProperties) {
		Map<String, Object> options = new LinkedHashMap<>();

		properties.stringPropertyNames()
				.stream()
				// Reject the properties which are implicitly available in Adhoc model
				.filter(k -> !IPostProcessor.UNDERLYING_MEASURES.equals(k))
				// There is not `real-time impacts` in Adhoc
				.filter(k -> !IPostProcessor.CONTINUOUS_QUERY_HANDLER_KEYS.equals(k))
				// Analysis levels are useless in Adhoc (and generally opaque in ActivePivot)
				.filter(k -> !AAdvancedPostProcessorV2.ANALYSIS_LEVELS_PROPERTY.equals(k))
				// leafLevels are not excluded as they hold some order which may be relied upon by some PostProcessors
				// Adhoc groupByColumns may be sorted (e.g. may be re-ordered lexicographically)
				.filter(k -> !Set.of(excludedProperties).contains(k))
				.forEach(key -> options.put(key, properties.get(key)));

		properties.entrySet()
				.stream()
				.filter(e -> !(e.getKey() instanceof String && e.getValue() instanceof String))
				.filter(e -> !Set.of(excludedProperties).contains(e.getKey()))
				// No interest in these properties, mapped to an int value
				.filter(e -> !AAdvancedPostProcessorV2.OUTPUT_TYPE.equals(e.getKey()))
				.filter(e -> !ADynamicAggregationPostProcessorV2.LEAF_TYPE.equals(e.getKey()))
				.forEach(entry -> options.put(String.valueOf(entry.getKey()), entry.getValue()));

		return options;
	}
}
