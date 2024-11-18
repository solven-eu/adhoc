package eu.solven.adhoc.from_file;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocMeasuresSet;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.Filtrator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.NonNull;
import smile.math.distance.EditDistance;

//An `implicit` measure is a measure which is not defined by itself, but as an underlying of another measure. It leads to deeper (hence more compact but more complex) trees
//An `anonymous` measure is a measure which has no name. It leads to more compact but less re-usable trees.
public class TestLoadDAGFromFile {
	// Used to generate a name for anonymous measures
	final AtomicInteger anonymousIndex = new AtomicInteger();

	@Test
	public void testFaultInKey_type() {
		Map<String, Object> input = Map.of("undelryings", List.of("k1", "k2"));

		Assertions.assertThatThrownBy(() -> getListParameter(input, "underlyings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Did you meant `undelryings` instead of `underlyings`");
	}

	@Test
	public void testFaultInKey_missing() {
		Map<String, Object> input = Map.of("type", "combinator");

		Assertions.assertThatThrownBy(() -> getListParameter(input, "underlyings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageNotContaining("instead of");
	}

	@Test
	public void testComplexImplicitMeasures() {
		Map<String, Object> input = Map.of("name",
				"k",
				"type",
				"combinator",
				"underlyings",
				List.of(Map.of("name",
						"k1",
						"type",
						"combinator",
						"underlyings",
						Arrays.asList(Map.of("name", "k11", "type", "combinator", "underlyings", Arrays.asList("k111")),
								Map.of("name", "k12", "type", "combinator", "underlyings", Arrays.asList("k121")))),
						Map.of("name",
								"k2",
								"type",
								"combinator",
								"underlyings",
								Arrays.asList(Map
										.of("name", "k21", "type", "combinator", "underlyings", Arrays.asList("k211")),
										Map.of("name",
												"k22",
												"type",
												"combinator",
												"underlyings",
												Arrays.asList("k221"))))));

		List<IMeasure> measures = makeMeasure(input);
		// TODO: Should these be defaulted?
		measures.addAll(makeMeasure(Map.of("name", "k111", "type", "aggregator")));
		measures.addAll(makeMeasure(Map.of("name", "k121", "type", "aggregator")));
		measures.addAll(makeMeasure(Map.of("name", "k211", "type", "aggregator")));
		measures.addAll(makeMeasure(Map.of("name", "k221", "type", "aggregator")));

		ListAssert<IMeasure> assertMeasures = Assertions.assertThat(measures).hasSize(11);
		assertMeasures.element(0)
				// The first measure must be the explicit measure
				.satisfies(m -> Assertions.assertThat(m.getName()).isEqualTo("k"));

		AdhocMeasuresSet ams = AdhocMeasuresSet.fromMeasures(measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = ams.makeMeasuresDag();
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(11);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(10);

		Assertions.assertThat(ams.resolveIfRef(ReferencedMeasure.builder().ref("k12").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k121");
				});
	}

	@Test
	public void testAnonymousUnderlyingNode() {
		Map<String, Object> input = Map.of("name",
				"k1Byk1k2",
				"type",
				"combinator",
				"combinationKey",
				"DIVIDE",
				"underlyings",
				List.of("k1",
						// The denominator is anonynous: it does not have as explicit name
						Map.of("type", "combinator", "underlyings", List.of("k1", "k2"))));

		List<IMeasure> measures = makeMeasure(input);
		measures.addAll(makeMeasure(Map.of("name", "k1", "type", "aggregator")));
		measures.addAll(makeMeasure(Map.of("name", "k2", "type", "aggregator")));

		ListAssert<IMeasure> assertMeasures = Assertions.assertThat(measures).hasSize(4);
		assertMeasures.element(0)
				// The first measure must be the explicit measure
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getName()).isEqualTo("k1Byk1k2");
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1", "anonymous-1");
				});

		AdhocMeasuresSet ams = AdhocMeasuresSet.fromMeasures(measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = ams.makeMeasuresDag();
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(4);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(4);

		Assertions.assertThat(ams.resolveIfRef(ReferencedMeasure.builder().ref("k1Byk1k2").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1", "anonymous-1");
				});
	}

	@Test
	public void testBasicFile() {
		Yaml yaml = new Yaml();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("dag_example.yml");
		Map<String, Object> obj = yaml.load(inputStream);
		// System.out.println(obj);

		Assertions.assertThat(obj).hasSize(1).containsKey("dags").extractingByKey("dags").satisfies(dags -> {
			Assertions.assertThat(dags)
					.isInstanceOf(Collection.class)
					.asInstanceOf(InstanceOfAssertFactories.COLLECTION)
					.satisfies(dagsCollection -> {
						Assertions.assertThat(dagsCollection).hasSize(1).element(0).satisfies(dagDetails -> {
							Assertions.assertThat(dagDetails)
									.asInstanceOf(InstanceOfAssertFactories.MAP)
									.satisfies(dagAsMap -> {
										Collection<Map<String, ?>> measures =
												MapPathGet.getRequiredAs(dagAsMap, "measures");

										AdhocMeasuresSet ame = measuresToAMS(measures);

										DirectedAcyclicGraph<IMeasure, DefaultEdge> jgrapht = ame.makeMeasuresDag();

										Assertions.assertThat(jgrapht.vertexSet()).hasSize(5);
										Assertions.assertThat(jgrapht.edgeSet()).hasSize(5);
									});
						});
					});
		});
	}

	private AdhocMeasuresSet measuresToAMS(Collection<Map<String, ?>> measures) {
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
	protected List<IMeasure> makeMeasure(Map<String, ?> measure) {
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

	public static String getStringParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredString(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public static Object getAnyParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	public static List<?> getListParameter(Map<String, ?> map, String key) {
		try {
			return MapPathGet.getRequiredAs(map, key);
		} catch (IllegalArgumentException e) {
			return onIllegalGet(map, key, e);
		}
	}

	private static <T> T onIllegalGet(Map<String, ?> map, String key, IllegalArgumentException e) {
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
}
