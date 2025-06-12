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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.model.Unfiltrator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import eu.solven.pepper.mappath.MapPathRemove;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps reading and writing {@link MeasureForest} into resource files.
 *
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class MeasureForestFromResource {
	public static final String K_TYPE = "type";
	private static final String K_NAME = "name";
	private static final String K_COMBINATION_OPTIONS = "combinationOptions";

	private static final List<String> SORTED_KEYS = ImmutableList.of(K_NAME,
			K_TYPE,
			"aggregationKey",
			"combinationKey",
			IHasCombinationKey.KEY_UNDERLYING_NAMES,
			"underlyingName");
	private static final Map<String, Integer> KEY_TO_INDEX =
			SORTED_KEYS.stream().collect(Collectors.toUnmodifiableMap(s -> s, SORTED_KEYS::indexOf));

	// Used to generate a name for anonymous measures
	final AtomicInteger anonymousIndex = new AtomicInteger();

	public MeasureForest loadToForest(String name, Collection<? extends Map<String, ?>> measuresAsMap) {
		List<IMeasure> measures = measuresAsMap.stream().flatMap(m -> makeMeasure(m).stream()).toList();

		return MeasureForest.builder().name(name).measures(measures).build();
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

		Optional<String> optName = MapPathGet.getOptionalString(measureAsMap, K_NAME);
		String name = optName.orElse("anonymous-" + anonymousIndex.getAndIncrement());

		mutableMeasureAsMap.put(K_NAME, name);

		IMeasure asMeasure = new ObjectMapper().convertValue(mutableMeasureAsMap, IMeasure.class);

		// The explicit measureAsMap has to be first in the output List
		measures.addFirst(asMeasure);

		return measures;
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

	public MeasureForest loadForestFromResource(String name, String format, Resource resource) throws IOException {
		ObjectMapper objectMapper = makeObjectMapper(format);

		try (InputStream inputStream = resource.getInputStream()) {
			List measures = objectMapper.readValue(inputStream, List.class);

			return makeForest(name, measures);
		}
	}

	protected ObjectMapper makeObjectMapper(String format) {
		return AdhocJackson.makeObjectMapper(format);
	}

	public MeasureForests loadForestsFromResource(String format, Resource resource) throws IOException {
		ObjectMapper objectMapper = makeObjectMapper(format);

		try (InputStream inputStream = resource.getInputStream()) {
			MeasureForests.MeasureForestsBuilder forests = MeasureForests.builder();
			List forestsAsList = objectMapper.readValue(inputStream, List.class);

			forestsAsList.forEach(forest -> {
				String name = MapPathGet.getRequiredString(forest, K_NAME);
				List measures = MapPathGet.getRequiredAs(forest, "measures");
				forests.forest(makeForest(name, measures));
			});

			return forests.build();
		}
	}

	protected MeasureForest makeForest(String name, List<Map<String, ?>> rawMeasures) {
		List<IMeasure> measures =
				rawMeasures.stream().flatMap(m -> makeMeasure(m).stream()).collect(Collectors.toList());

		return MeasureForest.fromMeasures(name, measures);
	}

	public String asString(String format, IMeasureForest forest) {
		ObjectMapper objectMapper = makeObjectMapper(format);

		List<?> asMaps = forest.getNameToMeasure()
				.values()
				.stream()
				.map(m -> asMap(objectMapper, m))
				.collect(Collectors.toList());

		try {
			return objectMapper.writeValueAsString(asMaps);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 *
	 * @param format
	 *            Typically json or yml. As defined by {@link AdhocJackson#makeObjectMapper(String)}.
	 * @param forests
	 * @return a String representing this {@link java.util.Set} of {@link IMeasureForest}
	 */
	public String asString(String format, MeasureForests forests) {
		ObjectMapper objectMapper = makeObjectMapper(format);

		List<Map<String, ?>> nameToForest = new ArrayList<>();

		forests.forestNames().forEach(forestName -> {
			IMeasureForest measures = forests.getForest(forestName);

			List<?> asMaps = measures.getNameToMeasure()
					.values()
					.stream()
					.map(m -> asMap(objectMapper, m))
					.collect(Collectors.toList());

			nameToForest.add(ImmutableMap.of(K_NAME, forestName, "measures", asMaps));
		});

		try {
			return objectMapper.writeValueAsString(nameToForest);
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
	@SuppressWarnings({ "PMD.AvoidDuplicateLiterals", "PMD.CognitiveComplexity" })
	protected Map<String, ?> simplifyProperties(IMeasure measure, Map<String, ?> map) {
		Comparator<String> comparing =
				Comparator.comparing(s -> Optional.ofNullable(KEY_TO_INDEX.get(s)).orElse(SORTED_KEYS.size()));
		Map<String, Object> clean = new TreeMap<>(comparing.thenComparing(s -> s));

		clean.putAll(map);

		if (measure instanceof Aggregator a) {
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
			MapPathRemove.remove(clean, K_COMBINATION_OPTIONS, IHasCombinationKey.KEY_MEASURE);
			if (Objects.equals(c.getCombinationOptions().get(IHasCombinationKey.KEY_UNDERLYING_NAMES),
					c.getUnderlyingNames())) {
				MapPathRemove.remove(clean, K_COMBINATION_OPTIONS, IHasCombinationKey.KEY_UNDERLYING_NAMES);
			}
			if (MapPathGet.getRequiredMap(clean, K_COMBINATION_OPTIONS).isEmpty()) {
				clean.remove(K_COMBINATION_OPTIONS);
			}
		} else if (measure instanceof Dispatchor d) {
			if (d.getAggregationOptions().isEmpty()) {
				clean.remove("aggregationOptions");
			}
		} else if (measure instanceof Partitionor b) {
			MapPathRemove.remove(clean, K_COMBINATION_OPTIONS, IHasCombinationKey.KEY_MEASURE);
			if (Objects.equals(b.getCombinationOptions().get(IHasCombinationKey.KEY_UNDERLYING_NAMES),
					b.getUnderlyingNames())) {
				MapPathRemove.remove(clean, K_COMBINATION_OPTIONS, IHasCombinationKey.KEY_UNDERLYING_NAMES);
			}
			if (Objects.equals(b.getCombinationOptions().get(IHasCombinationKey.KEY_GROUPBY_COLUMNS),
					b.getGroupBy().getGroupedByColumns())) {
				MapPathRemove.remove(clean, K_COMBINATION_OPTIONS, IHasCombinationKey.KEY_GROUPBY_COLUMNS);
			}
			if (MapPathGet.getRequiredMap(clean, K_COMBINATION_OPTIONS).isEmpty()) {
				clean.remove(K_COMBINATION_OPTIONS);
			}

			if (b.getAggregationOptions().isEmpty()) {
				clean.remove("aggregationOptions");
			}

		} else if (measure instanceof Filtrator f) {
			log.trace("Keep this branch to write short `type`: {}", f);
		} else if (measure instanceof Unfiltrator u) {
			log.trace("Keep this branch to write short `type`: {}", u);
		} else if (measure instanceof Shiftor s) {
			log.trace("Keep this branch to write short `type`: {}", s);
		} else if (measure instanceof Columnator c) {
			log.trace("Keep this branch to write short `type`: {}", c);
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

		// This workaround a side-effect of https://github.com/FasterXML/jackson-databind/issues/5030
		asMap.put(K_TYPE, measure.getClass().getName());
	}
}
