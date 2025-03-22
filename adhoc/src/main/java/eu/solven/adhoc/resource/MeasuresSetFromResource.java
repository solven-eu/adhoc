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
package eu.solven.adhoc.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.springframework.core.io.Resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.measure.AdhocBagOfMeasureBag;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import eu.solven.pepper.mappath.MapPathRemove;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps reading and writing {@link AdhocMeasureBag} into resource files.
 *
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class MeasuresSetFromResource {
	public static final String KEY_TYPE = "type";

	private static final List<String> sortedKeys = List.of("name",
			KEY_TYPE,
			"aggregationKey",
			"combinationKey",
			IHasCombinationKey.KEY_UNDERLYING_NAMES,
			"underlyingName");
	private static final Map<String, Integer> keyToIndex =
			sortedKeys.stream().collect(Collectors.toUnmodifiableMap(s -> s, sortedKeys::indexOf));

	// Used to generate a name for anonymous measures
	final AtomicInteger anonymousIndex = new AtomicInteger();

	public AdhocMeasureBag loadToBag(String name, Collection<? extends Map<String, ?>> measuresAsMap) {
		List<IMeasure> measures = measuresAsMap.stream().flatMap(m -> makeMeasure(m).stream()).toList();

		return AdhocMeasureBag.builder().name(name).measures(measures).build();
	}

	/**
	 * @param measureAsMap
	 *            The measure as read from some configuration file. Typically produced with Jackson.
	 * @return a {@link List} of measures. There may be multiple measureAsMap if the explicit measureAsMap defines
	 *         underlying measures. The explicit measureAsMap is always first in the output list.
	 */
	public List<IMeasure> makeMeasure(Map<String, ?> measureAsMap) {
		List<IMeasure> measures = new ArrayList<>();

		Map<String, Object> mutableMeasureAsMap = new LinkedHashMap<>(measureAsMap);

		// The following section unnest recursive measure definitions
		{
			Optional<List<?>> optRawUnderlyings = MapPathGet.getOptionalAs(measureAsMap, "underlyings");

			optRawUnderlyings.ifPresent(rawUnderlyings -> {
				List<String> underlyingNames = rawUnderlyings.stream().map(rawUnderlying -> {
					return registerMeasuresReturningMainOne(rawUnderlying, measures);
				}).collect(Collectors.toList());

				mutableMeasureAsMap.put("underlyings", underlyingNames);
			});
		}
		{
			Optional<?> optRawUnderlying = MapPathGet.getOptionalAs(measureAsMap, "underlying");

			optRawUnderlying.ifPresent(rawUnderlying -> {
				String underlyingName = registerMeasuresReturningMainOne(rawUnderlying, measures);

				mutableMeasureAsMap.put("underlying", underlyingName);
			});
		}

		Optional<String> optName = MapPathGet.getOptionalString(measureAsMap, "name");
		String name = optName.orElse("anonymous-" + anonymousIndex.getAndIncrement());

		mutableMeasureAsMap.put("name", name);

		IMeasure asMeasure = new ObjectMapper().convertValue(mutableMeasureAsMap, IMeasure.class);

		// The explicit measureAsMap has to be first in the output List
		measures.addFirst(asMeasure);

		return measures;
	}

	/**
	 * @param type
	 * @param measureAsMap
	 * @param measures
	 * @param name
	 * @return the default behavior is to throw
	 */
	protected IMeasure onUnknownType(String type, Map<String, ?> measureAsMap, List<IMeasure> measures, String name) {
		Class<? extends IMeasure> clazz;
		try {
			clazz = (Class<? extends IMeasure>) Class.forName(type);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Issue loading %s".formatted(type), e);
		}

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/4486787/jackson-with-json-unrecognized-field-not-marked-as-ignorable
		// objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		try {
			measureAsMap = new LinkedHashMap<>(measureAsMap);
			measureAsMap.remove(KEY_TYPE);

			IMeasure measure = objectMapper.convertValue(measureAsMap, clazz);

			return measure;
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Unexpected value: " + type, e);
		}
	}

	protected IMeasure makeDispatchor(Map<String, ?> measure, List<IMeasure> measures, String name) {
		Object rawUnderlying = getAnyParameter(measure, "underlying");

		String underlyingName = registerMeasuresReturningMainOne(rawUnderlying, measures);

		Dispatchor.DispatchorBuilder builder = Dispatchor.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlying(underlyingName);
		MapPathGet.getOptionalString(measure, "aggregationKey").ifPresent(builder::aggregationKey);

		String decompositionKey = getStringParameter(measure, "decompositionKey");
		builder.decompositionKey(decompositionKey);

		MapPathGet.<Map<String, ?>>getOptionalAs(measure, "decompositionOptions")
				.ifPresent(builder::decompositionOptions);

		return builder.build();
	}

	protected IMeasure makeBucketor(Map<String, ?> measure, List<IMeasure> measures, String name) {
		List<?> rawUnderlyings = getListParameter(measure, "underlyings");

		List<String> underlyingNames = rawUnderlyings.stream().map(rawUnderlying -> {
			return registerMeasuresReturningMainOne(rawUnderlying, measures);
		}).collect(Collectors.toList());

		Bucketor.BucketorBuilder builder = Bucketor.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlyings(underlyingNames);

		MapPathGet.getOptionalString(measure, "aggregationKey").ifPresent(builder::aggregationKey);

		MapPathGet.getOptionalString(measure, "combinationKey").ifPresent(builder::combinationKey);
		MapPathGet.<Map<String, ?>>getOptionalAs(measure, "combinationOptions").ifPresent(builder::combinationOptions);

		Object rawGroupBy = getAnyParameter(measure, "groupBy");
		builder.groupBy(toGroupBy(rawGroupBy));

		return builder.build();
	}

	protected @NonNull IAdhocGroupBy toGroupBy(Object rawGroupBy) {
		if (rawGroupBy instanceof List<?> wildcards) {
			List<ReferencedColumn> adhocColumns = wildcards.stream().map(columnDefinition -> {
				if (columnDefinition instanceof String asString) {
					return ReferencedColumn.ref(asString);
				} else {
					// CalculatedColumn
					throw new UnsupportedOperationException("TODO");
				}
			}).toList();
			return GroupByColumns.of(adhocColumns);
		} else {
			throw new UnsupportedOperationException(
					"TODO: manage %s".formatted(PepperLogHelper.getObjectAndClass(rawGroupBy)));
		}
	}

	protected IMeasure makeFiltrator(Map<String, ?> measure, List<IMeasure> measures, String name) {
		// Filtrator has a single underlying measure
		Object rawUnderlying = getAnyParameter(measure, "underlying");

		String underlyingName = registerMeasuresReturningMainOne(rawUnderlying, measures);

		Filtrator.FiltratorBuilder builder = Filtrator.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlying(underlyingName);

		Map<String, ?> rawFilter = getMapParameter(measure, "filter");
		builder.filter(toFilter(rawFilter));

		return builder.build();
	}

	protected IMeasure makeUnfiltrator(Map<String, ?> measure, List<IMeasure> measures, String name) {
		// Unfiltrator has a single underlying measure
		Object rawUnderlying = getAnyParameter(measure, "underlying");

		String underlyingName = registerMeasuresReturningMainOne(rawUnderlying, measures);

		Unfiltrator.UnfiltratorBuilder builder = Unfiltrator.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlying(underlyingName);

		List<String> unfiltered = (List<String>) getListParameter(measure, "unfiltereds");
		builder.unfiltereds(unfiltered);

		return builder.build();
	}

	protected IMeasure makeShiftor(Map<String, ?> measure, List<IMeasure> measures, String name) {
		// Filtrator has a single underlying measure
		Object rawUnderlying = getAnyParameter(measure, "underlying");

		String underlyingName = registerMeasuresReturningMainOne(rawUnderlying, measures);

		Shiftor.ShiftorBuilder builder = Shiftor.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlying(underlyingName);

		MapPathGet.getOptionalString(measure, "editorKey").ifPresent(builder::editorKey);
		MapPathGet.<Map<String, ?>>getOptionalAs(measure, "editorOptions").ifPresent(builder::editorOptions);

		return builder.build();
	}

	protected IMeasure makeCombinator(Map<String, ?> measure, List<IMeasure> measures, String name) {
		List<?> rawUnderlyings = getListParameter(measure, "underlyings");

		List<String> underlyingNames = rawUnderlyings.stream().map(rawUnderlying -> {
			return registerMeasuresReturningMainOne(rawUnderlying, measures);
		}).collect(Collectors.toList());

		Combinator.CombinatorBuilder builder = Combinator.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()))
				.underlyings(underlyingNames);

		MapPathGet.getOptionalString(measure, "combinationKey").ifPresent(builder::combinationKey);
		MapPathGet.<Map<String, ?>>getOptionalAs(measure, "combinationOptions").ifPresent(builder::combinationOptions);

		return builder.build();
	}

	protected IMeasure makeAggregator(Map<String, ?> measure, String name) {
		Aggregator.AggregatorBuilder builder = Aggregator.builder()
				.name(name)
				.tags(MapPathGet.<List<String>>getOptionalAs(measure, "tags").orElse(List.of()));

		MapPathGet.getOptionalString(measure, "columnName").ifPresent(builder::columnName);
		MapPathGet.getOptionalString(measure, "aggregationKey").ifPresent(builder::aggregationKey);

		return builder.build();
	}

	protected String registerMeasuresReturningMainOne(Object rawUnderlying, List<IMeasure> measures) {
		if (rawUnderlying instanceof String asString) {
			return asString;
		} else if (rawUnderlying instanceof Map<?, ?> asMap) {
			List<IMeasure> underlyingMeasures = makeMeasure((Map) asMap);

			measures.addAll(underlyingMeasures);

			return underlyingMeasures.getFirst().getName();
		} else {
			throw new IllegalArgumentException(
					"Invalid underlying: %s".formatted(PepperLogHelper.getObjectAndClass(rawUnderlying)));
		}
	}

	protected @NonNull IAdhocFilter toFilter(Map<String, ?> rawFilter) {
		ObjectMapper objectMapper = new ObjectMapper();

		return objectMapper.convertValue(rawFilter, IAdhocFilter.class);
	}

	protected Map<String, ?> getMapParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredMap(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public String getStringParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredString(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public Object getAnyParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public List<?> getListParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	protected <T> T onIllegalGet(Map<String, ?> map, String key, IllegalArgumentException e) {
		if (map.isEmpty()) {
			throw new IllegalArgumentException("input map is empty while looking for %s".formatted(key));
		} else if (e.getMessage().contains("(key not present)")) {
			String minimizingDistance = minimizingDistance(map.keySet(), key);

			if (SmileEditDistance.levenshtein(minimizingDistance, key) <= 2) {
				throw new IllegalArgumentException(
						"Did you mean `%s` instead of `%s`".formatted(minimizingDistance, key),
						e);
			} else {
				// It seems we're rather missing the input than having a typo
				throw e;
			}
		} else {
			throw e;
		}
	}

	/**
	 *
	 * @param options
	 * @param key
	 * @return the option minimizing its distance to the requested key.
	 */
	public static String minimizingDistance(Collection<String> options, String key) {
		String minimizingDistance =
				options.stream().min(Comparator.comparing(s -> SmileEditDistance.levenshtein(s, key))).orElse("?");
		return minimizingDistance;
	}

	public AdhocMeasureBag loadBagFromResource(String name, String format, Resource resource) throws IOException {
		ObjectMapper objectMapper = makeObjectMapper(format);

		try (InputStream inputStream = resource.getInputStream()) {
			List measures = objectMapper.readValue(inputStream, List.class);

			return makeBag(name, measures);
		}
	}

	protected ObjectMapper makeObjectMapper(String format) {
		return AdhocJackson.makeObjectMapper(format);
	}

	public AdhocBagOfMeasureBag loadMapFromResource(String format, Resource resource) throws IOException {
		ObjectMapper objectMapper = makeObjectMapper(format);

		try (InputStream inputStream = resource.getInputStream()) {
			AdhocBagOfMeasureBag abmb = new AdhocBagOfMeasureBag();
			List bags = objectMapper.readValue(inputStream, List.class);

			bags.forEach(bag -> {
				String name = MapPathGet.getRequiredString(bag, "name");
				List measures = MapPathGet.getRequiredAs(bag, "measures");
				abmb.putBag(name, makeBag(name, measures));
			});

			return abmb;
		}
	}

	protected AdhocMeasureBag makeBag(String name, List<Map<String, ?>> rawMeasures) {
		List<IMeasure> measures =
				rawMeasures.stream().flatMap(m -> makeMeasure(m).stream()).collect(Collectors.toList());

		return AdhocMeasureBag.fromMeasures(name, measures);
	}

	public String asString(String format, IMeasureForest amb) {
		ObjectMapper objectMapper = makeObjectMapper(format);

		List<?> asMaps =
				amb.getNameToMeasure().values().stream().map(m -> asMap(objectMapper, m)).collect(Collectors.toList());

		try {
			return objectMapper.writeValueAsString(asMaps);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public String asString(String format, AdhocBagOfMeasureBag abmb) {
		ObjectMapper objectMapper = makeObjectMapper(format);

		List<Map<String, ?>> bagNameToMeasures = new ArrayList<>();

		abmb.bagNames().forEach(bagName -> {
			IMeasureForest measures = abmb.getBag(bagName);

			List<?> asMaps = measures.getNameToMeasure()
					.values()
					.stream()
					.map(m -> asMap(objectMapper, m))
					.collect(Collectors.toList());

			bagNameToMeasures.add(ImmutableMap.of("name", bagName, "measures", asMaps));
		});

		try {
			return objectMapper.writeValueAsString(bagNameToMeasures);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	// https://stackoverflow.com/questions/25387978/how-to-add-custom-deserializer-to-interface-using-jackson
	public Map<String, ?> asMap(ObjectMapper objectMapper, IMeasure m) {
		// https://github.com/FasterXML/jackson-databind/issues/4983
		Map rawMap = objectMapper.convertValue(m, Map.class);
		// objectMapper.readValue(objectMapper.writerFor(IMeasure.class).writeValueAsString(m), Map.class);

		// Let's remove some redundant properties, as the output Map will typically be read by humans
		return simplifyProperties(m, rawMap);
	}

	/**
	 * This is useful to generate human-friendly configuration, not including all implicit configuration.
	 * 
	 * It will also add a `type` field indicating the type of measure.
	 *
	 * @param measure
	 *            the {@link IMeasure} object
	 * @param map
	 *            the initial serialized view of {@link IMeasure}
	 * @return a stripped version of the {@link Map}, where implied properties are removed.
	 */
	protected Map<String, ?> simplifyProperties(IMeasure measure, Map<String, ?> map) {
		Comparator<String> comparing =
				Comparator.comparing(s -> Optional.ofNullable(keyToIndex.get(s)).orElse(sortedKeys.size()));
		Map<String, Object> clean = new TreeMap<>(comparing.thenComparing(s -> s));

		clean.putAll(map);

		if (measure instanceof Aggregator a) {
			// clean.put(KEY_TYPE, "aggregator");
			if (SumAggregation.KEY.equals(a.getAggregationKey())) {
				clean.remove("aggregationKey");
			}
			if (a.getColumnName().equals(a.getName())) {
				clean.remove("columnName");
			}
			if (a.getAggregationOptions().isEmpty()) {
				clean.remove("aggregationOptions");
			}
		} else if (measure instanceof Combinator c) {
			// clean.put(KEY_TYPE, "combinator");

			MapPathRemove.remove(clean, "combinationOptions", IHasCombinationKey.KEY_MEASURE);
			if (Objects.equals(c.getCombinationOptions().get(IHasCombinationKey.KEY_UNDERLYING_NAMES),
					c.getUnderlyingNames())) {
				MapPathRemove.remove(clean, "combinationOptions", IHasCombinationKey.KEY_UNDERLYING_NAMES);
			}
			if (MapPathGet.getRequiredMap(clean, "combinationOptions").isEmpty()) {
				clean.remove("combinationOptions");
			}
		} else if (measure instanceof Filtrator f) {
			// clean.put(KEY_TYPE, "filtrator");
		} else if (measure instanceof Unfiltrator u) {
			// clean.put(KEY_TYPE, "unfiltrator");
		} else if (measure instanceof Shiftor s) {
			// clean.put(KEY_TYPE, "shiftor");
		} else if (measure instanceof Dispatchor d) {
			// clean.put(KEY_TYPE, "dispatchor");

			if (d.getAggregationOptions().isEmpty()) {
				clean.remove("aggregationOptions");
			}
		} else if (measure instanceof Bucketor b) {
			// clean.put(KEY_TYPE, "bucketor");

			MapPathRemove.remove(clean, "combinationOptions", IHasCombinationKey.KEY_MEASURE);
			if (Objects.equals(b.getCombinationOptions().get(IHasCombinationKey.KEY_UNDERLYING_NAMES),
					b.getUnderlyingNames())) {
				MapPathRemove.remove(clean, "combinationOptions", IHasCombinationKey.KEY_UNDERLYING_NAMES);
			}
			if (Objects.equals(b.getCombinationOptions().get(IHasCombinationKey.KEY_GROUPBY_COLUMNS),
					b.getGroupBy().getGroupedByColumns())) {
				MapPathRemove.remove(clean, "combinationOptions", IHasCombinationKey.KEY_GROUPBY_COLUMNS);
			}
			if (MapPathGet.getRequiredMap(clean, "combinationOptions").isEmpty()) {
				clean.remove("combinationOptions");
			}

			if (b.getAggregationOptions().isEmpty()) {
				clean.remove("aggregationOptions");
			}
		} else {
			onUnknownMeasureType(measure, clean);
		}

		if (measure.getTags().isEmpty()) {
			clean.remove("tags");
		}

		return clean;
	}

	protected void onUnknownMeasureType(IMeasure measure, Map<String, Object> asMap) {
		log.warn("Unknown measureType: {}", measure);

		asMap.put(KEY_TYPE, measure.getClass().getName());
	}
}
