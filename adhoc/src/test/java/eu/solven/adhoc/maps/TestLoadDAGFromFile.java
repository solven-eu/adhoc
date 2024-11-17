package eu.solven.adhoc.maps;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import eu.solven.adhoc.dag.AdhocMeasuresSet;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.pepper.mappath.MapPathGet;

public class TestLoadDAGFromFile {
	@Test
	public void testBasicFile() {
		Yaml yaml = new Yaml();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("dag_example.yml");
		Map<String, Object> obj = yaml.load(inputStream);
		System.out.println(obj);

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

										// AdhocQueryEngine aqe =
										// AdhocQueryEngine.builder().measuresSet(ame).eventBus(new
										// EventBus()).build();

										DirectedAcyclicGraph<IMeasure, DefaultEdge> jgrapht = ame.makeMeasuresDag();

										Assertions.assertThat(jgrapht.vertexSet()).hasSize(3);
										Assertions.assertThat(jgrapht.edgeSet()).hasSize(2);
									});
						});
					});
		});
	}

	private AdhocMeasuresSet measuresToAMS(Collection<Map<String, ?>> measures) {
		Map<String, IMeasure> nameToMeasure = measures.stream().map(measure -> {
			String type = MapPathGet.getRequiredString(measure, "type");
			String name = MapPathGet.getRequiredString(measure, "name");

			return switch (type) {
			case "aggregator": {
				yield Aggregator.builder().name(name).build();
			}
			case "combinator": {
				yield Combinator.builder()
						.name(name)
						.underlyingNames(MapPathGet.getRequiredAs(measure, "underlyings"))
						.build();
			}
			default:
				throw new IllegalArgumentException("Unexpected value: " + type);
			};
		}).collect(Collectors.toMap(m -> m.getName(), m -> m));

		AdhocMeasuresSet ame = AdhocMeasuresSet.builder().nameToMeasure(nameToMeasure).build();
		return ame;
	}
}
