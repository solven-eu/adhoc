package eu.solven.adhoc.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.Yaml;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocMeasuresSet;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.NonNull;
import smile.math.distance.EditDistance;

public class MeasuresSetFromResource {
	// Used to generate a name for anonymous measures
	final AtomicInteger anonymousIndex = new AtomicInteger();

	public AdhocMeasuresSet measuresToAMS(Collection<? extends Map<String, ?>> measures) {
		Map<String, IMeasure> nameToMeasure = measures.stream().flatMap(measure -> {
			return makeMeasure(measure).stream();
		}).collect(Collectors.toMap(m -> m.getName(), m -> m));

		AdhocMeasuresSet ame = AdhocMeasuresSet.builder().nameToMeasure(nameToMeasure).build();
		return ame;
	}

	/**
	 * 
	 * @param measure
	 *            never empty;
	 * @return a {@link List} of measures. There may be multiple measure if the explicit measure defines underlying
	 *         measures. The explicit measure is always first i nthe output list.
	 */
	public List<IMeasure> makeMeasure(Map<String, ?> measure) {
		List<IMeasure> measures = new ArrayList<>();

		String type = getStringParameter(measure, "type");
		String name =
				MapPathGet.getOptionalString(measure, "name").orElse("anonymous-" + anonymousIndex.getAndIncrement());

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
			yield Combinator.builder().name(name).underlyingNames(underlyingNames).build();
		}
		case "filtrator":

		{
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

	public Map<String, ?> loadMapFromResource(Resource resource) throws IOException {
		Yaml yaml = new Yaml();
		try (InputStream inputStream = resource.getInputStream()) {
			Map<String, Object> obj = yaml.load(inputStream);

			return obj;
		}
	}
}
