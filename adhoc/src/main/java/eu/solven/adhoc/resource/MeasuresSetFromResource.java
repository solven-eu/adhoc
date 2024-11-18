package eu.solven.adhoc.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.springframework.core.io.Resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocBagOfMeasureBag;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Dispatchor;
import eu.solven.adhoc.transformers.Filtrator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import eu.solven.pepper.mappath.MapPathRemove;
import lombok.NonNull;
import smile.math.distance.EditDistance;

public class MeasuresSetFromResource {
	// Used to generate a name for anonymous measures
	final AtomicInteger anonymousIndex = new AtomicInteger();

	public AdhocMeasureBag measuresToAMS(Collection<? extends Map<String, ?>> measures) {
		Map<String, IMeasure> nameToMeasure = measures.stream().flatMap(measure -> {
			return makeMeasure(measure).stream();
		}).collect(Collectors.toMap(m -> m.getName(), m -> m));

		AdhocMeasureBag ame = AdhocMeasureBag.builder().nameToMeasure(nameToMeasure).build();
		return ame;
	}

	/**
	 * 
	 * @param measure
	 *            never empty;
	 * @return a {@link List} of measures. There may be multiple measure if the explicit measure defines underlying
	 *         measures. The explicit measure is always first in the output list.
	 */
	public List<IMeasure> makeMeasure(Map<String, ?> measure) {
		List<IMeasure> measures = new ArrayList<>();

		String type = getStringParameter(measure, "type");
		Optional<String> optName = MapPathGet.getOptionalString(measure, "name");
		String name = optName.orElse("anonymous-" + anonymousIndex.getAndIncrement());

		IMeasure asMeasure = switch (type) {
		case "aggregator": {
			yield Aggregator.builder().name(name).build();
		}
		case "combinator": {
			List<?> rawUnderlyings = getListParameter(measure, "underlyings");

			List<String> underlyingNames = rawUnderlyings.stream().map(rawUnderlying -> {
				if (rawUnderlying instanceof String asString) {
					return asString;
				} else if (rawUnderlying instanceof Map<?, ?> asMap) {
					List<IMeasure> underlyingMeasures = makeMeasure((Map) asMap);

					measures.addAll(underlyingMeasures);

					return underlyingMeasures.getFirst().getName();
				} else {
					throw new IllegalArgumentException(
							"Invalid underying: %s".formatted(PepperLogHelper.getObjectAndClass(rawUnderlying)));
				}

			}).collect(Collectors.toList());

			// optName

			// if (optName.isEmpty()) {
			// builder.tag("anonymous");
			// }
			yield Combinator.forceBuilder().name(name).underlyingNames(underlyingNames).build();
		}
		case "filtrator": {
			Object rawUnderlying = getAnyParameter(measure, "underlying");

			String undelryingName;
			if (rawUnderlying instanceof String asString) {
				undelryingName = asString;
			} else if (rawUnderlying instanceof Map<?, ?> asMap) {
				List<IMeasure> underlyingMeasures = makeMeasure((Map) asMap);

				measures.addAll(underlyingMeasures);

				undelryingName = underlyingMeasures.getFirst().getName();
			} else {
				throw new IllegalArgumentException(
						"Invalid underying: %s".formatted(PepperLogHelper.getObjectAndClass(rawUnderlying)));
			}

			Map<String, ?> rawFilter = getMapParameter(measure, "filter");

			yield Filtrator.builder().name(name).underlyingName(undelryingName).filter(toFilter(rawFilter)).build();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + type);
		};

		// The explicit measure has to be first in the output List
		measures.add(0, asMeasure);

		return measures;
	}

	private @NonNull IAdhocFilter toFilter(Map<String, ?> rawFilter) {
		String type = getStringParameter(rawFilter, "type");

		return switch (type) {
		case "match": {
			yield ColumnFilter.isEqualTo(getStringParameter(rawFilter, "column"),
					getStringParameter(rawFilter, "matching"));
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + type);
		};
	}

	private Map<String, ?> getMapParameter(Map<String, ?> map, String key) {
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

	private <T> T onIllegalGet(Map<String, ?> map, String key, IllegalArgumentException e) {
		if (map.isEmpty()) {
			throw new IllegalArgumentException("input map is empty while looking for %s".formatted(key));
		} else if (e.getMessage().contains("(key not present)")) {
			String minimizingDistance =
					map.keySet().stream().min(Comparator.comparing(s -> EditDistance.levenshtein(s, key))).orElse("?");

			if (EditDistance.levenshtein(minimizingDistance, key) <= 2) {
				throw new IllegalArgumentException(
						"Did you meant `%s` instead of `%s`".formatted(minimizingDistance, key),
						e);
			} else {
				// It seems we're rather missing the input than having a typo
				throw e;
			}
		} else {
			throw e;
		}
	}

	public AdhocBagOfMeasureBag loadMapFromResource(String format, Resource resource) throws IOException {
		ObjectMapper objectMapper;
		if ("yml".equalsIgnoreCase(format) || "yaml".equalsIgnoreCase(format)) {
			objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
		} else {
			objectMapper = new ObjectMapper();
		}

		try (InputStream inputStream = resource.getInputStream()) {
			AdhocBagOfMeasureBag abmb = new AdhocBagOfMeasureBag();
			List bags = objectMapper.readValue(inputStream, List.class);

			bags.forEach(bag -> {
				String name = MapPathGet.getRequiredString(bag, "name");
				List measures = MapPathGet.getRequiredAs(bag, "measures");
				abmb.putBag(name, makeBag(measures));
			});

			return abmb;
		}
	}

	private AdhocMeasureBag makeBag(List<Map<String, ?>> rawMeasures) {
		List<IMeasure> measures = rawMeasures.stream().flatMap(m -> {
			return makeMeasure(m).stream();
		}).collect(Collectors.toList());

		return AdhocMeasureBag.fromMeasures(measures);
	}

	public String asString(String format, AdhocBagOfMeasureBag abmb) {
		ObjectMapper objectMapper;
		if ("yml".equalsIgnoreCase(format) || "yaml".equalsIgnoreCase(format)) {
			objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
		} else {
			objectMapper = new ObjectMapper();
		}

		List<Map<String, ?>> bagNameToMeasures = new ArrayList<>();

		abmb.bagNames().forEach(bagName -> {
			AdhocMeasureBag ams = abmb.getBag(bagName);

			List<?> asMaps = ams.getNameToMeasure()
					.values()
					.stream()
					.map(m -> removeUselessProperties(m, objectMapper.convertValue(m, Map.class)))
					.collect(Collectors.toList());

			bagNameToMeasures.add(ImmutableMap.of("name", bagName, "measures", asMaps));
		});

		try {
			return objectMapper.writeValueAsString(bagNameToMeasures);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static final List<String> sortedKeys =
			List.of("name", "type", "aggregationKey", "combinationKey", "underlyingNames", "underlyingName");
	private static final Map<String, Integer> keyToIndex =
			sortedKeys.stream().collect(Collectors.toMap(s -> s, s -> sortedKeys.indexOf(s)));

	private Map<String, ?> removeUselessProperties(IMeasure m, Map<String, ?> map) {
		Comparator<String> comparing =
				Comparator.comparing(s -> Optional.ofNullable(keyToIndex.get(s)).orElse(sortedKeys.size()));
		Map<String, Object> clean = new TreeMap<>(comparing.thenComparing(s -> s));

		clean.putAll(map);

		if (m instanceof Aggregator a) {
			clean.put("type", "aggregator");
			if (a.getAggregationKey().equals(SumAggregator.KEY)) {
				clean.remove("aggregationKey");
			}
			if (a.getColumnName().equals(a.getName())) {
				clean.remove("columnName");
			}
		} else if (m instanceof Combinator c) {
			clean.put("type", "combinator");

			if (c.getCombinationOptions().get("underlyingNames").equals(c.getUnderlyingNames())) {
				MapPathRemove.remove(clean, "combinationOptions", "underlyingNames");
			}
			if (MapPathGet.getRequiredMap(clean, "combinationOptions").isEmpty()) {
				clean.remove("combinationOptions");
			}

			clean.put("underlyings", clean.remove("underlyingNames"));
		} else if (m instanceof Filtrator f) {
			clean.put("type", "filtrator");
			clean.put("underlying", clean.remove("underlyingName"));
		} else if (m instanceof Dispatchor d) {
			clean.put("type", "dispatchor");
		} else {
			throw new UnsupportedOperationException("Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(m)));
		}

		if (m.getTags().isEmpty()) {
			clean.remove("tags");
		}
		// if (m.isDebug()) {

		return clean;
	}
}
