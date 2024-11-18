package eu.solven.adhoc.from_file;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import eu.solven.adhoc.dag.AdhocMeasuresSet;
import eu.solven.adhoc.resource.MeasuresSetFromResource;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.pepper.mappath.MapPathGet;

//An `implicit` measure is a measure which is not defined by itself, but as an underlying of another measure. It leads to deeper (hence more compact but more complex) trees
//An `anonymous` measure is a measure which has no name. It leads to more compact but less re-usable trees.
public class TestMeasuresSetFromResource {
	final MeasuresSetFromResource fromResource = new MeasuresSetFromResource();

	@Test
	public void testFaultInKey_type() {
		Map<String, Object> input = Map.of("undelryings", List.of("k1", "k2"));

		Assertions.assertThatThrownBy(() -> fromResource.getListParameter(input, "underlyings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Did you meant `undelryings` instead of `underlyings`");
	}

	@Test
	public void testFaultInKey_missing() {
		Map<String, Object> input = Map.of("type", "combinator");

		Assertions.assertThatThrownBy(() -> fromResource.getListParameter(input, "underlyings"))
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

		List<IMeasure> measures = fromResource.makeMeasure(input);
		// TODO: Should these be defaulted?
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k111", "type", "aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k121", "type", "aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k211", "type", "aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k221", "type", "aggregator")));

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

		List<IMeasure> measures = fromResource.makeMeasure(input);
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k1", "type", "aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k2", "type", "aggregator")));

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
	public void testBasicFile() throws IOException {
		Map<String, ?> obj = fromResource.loadMapFromResource(new ClassPathResource("dag_example.yml"));

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

										AdhocMeasuresSet ame = fromResource.measuresToAMS(measures);

										DirectedAcyclicGraph<IMeasure, DefaultEdge> jgrapht = ame.makeMeasuresDag();

										Assertions.assertThat(jgrapht.vertexSet()).hasSize(5);
										Assertions.assertThat(jgrapht.edgeSet()).hasSize(5);
									});
						});
					});
		});
	}
}
