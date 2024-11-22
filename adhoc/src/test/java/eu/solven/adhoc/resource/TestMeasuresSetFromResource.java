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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dag.AdhocBagOfMeasureBag;
import eu.solven.adhoc.dag.AdhocMeasureBag;
import eu.solven.adhoc.transformers.Combinator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;

//An `implicit` measure is a measure which is not defined by itself, but as an underlying of another measure. It leads to deeper (hence more compact but more complex) trees
//An `anonymous` measure is a measure which has no name. It leads to more compact but less re-usable trees.
public class TestMeasuresSetFromResource {
	final MeasuresSetFromResource fromResource = new MeasuresSetFromResource();

	@Test
	public void testFaultInKey_type() {
		Map<String, Object> input = Map.of("underlyings", List.of("k1", "k2"));

		Assertions.assertThatThrownBy(() -> fromResource.getListParameter(input, "undelryings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Did you meant `underlyings` instead of `undelryings`");
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
		Map<String, Object> input = ImmutableMap.<String, Object>builder()
				.put("name", "k")
				.put("type", "combinator")
				.put("underlyings",
						List.of(ImmutableMap.<String, Object>builder()
								.put("name", "k1")
								.put("type", "combinator")
								.put("underlyings",
										Arrays.asList(
												Map.of("name",
														"k11",
														"type",
														"combinator",
														"underlyings",
														Arrays.asList("k111")),
												Map.of("name",
														"k12",
														"type",
														"combinator",
														"underlyings",
														Arrays.asList("k121"))))
								.build(),
								ImmutableMap.<String, Object>builder()
										.put("name", "k2")
										.put("type", "combinator")
										.put("underlyings",
												Arrays.asList(
														Map.of("name",
																"k21",
																"type",
																"combinator",
																"underlyings",
																Arrays.asList("k211")),
														Map.of("name",
																"k22",
																"type",
																"combinator",
																"underlyings",
																Arrays.asList("k221"))))
										.build()))
				.build();

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

		AdhocMeasureBag ams = AdhocMeasureBag.fromMeasures(measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = ams.makeMeasuresDag();
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(11);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(10);

		Assertions.assertThat(ams.resolveIfRef(ReferencedMeasure.builder().ref("k12").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k121");
				});
	}

	@Test
	public void testAnonymousUnderlyingNode() throws IOException {
		Map<String, Object> input = ImmutableMap.<String, Object>builder()
				.put("name", "k1Byk1k2")
				.put("type", "combinator")
				.put("combinationKey", "DIVIDE")
				.put("underlyings",
						List.of("k1",
								// The denominator is anonynous: it does not have as explicit name
								Map.of("type", "combinator", "underlyings", List.of("k1", "k2"))))
				.build();

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

		AdhocMeasureBag ams = AdhocMeasureBag.fromMeasures(measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = ams.makeMeasuresDag();
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(4);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(4);

		Assertions.assertThat(ams.resolveIfRef(ReferencedMeasure.builder().ref("k1Byk1k2").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1", "anonymous-1");
				});

		{
			AdhocBagOfMeasureBag bagOfBag = new AdhocBagOfMeasureBag();
			bagOfBag.putBag("someBagName", ams);

			String amsAsString = fromResource.asString("yml", bagOfBag);
			Assertions.assertThat(amsAsString).isEqualTo("""
					- name: "someBagName"
					  measures:
					  - name: "k1Byk1k2"
					    type: "combinator"
					    combinationKey: "SUM"
					    underlyings:
					    - "k1"
					    - "anonymous-1"
					  - name: "k1"
					    type: "aggregator"
					  - name: "k2"
					    type: "aggregator"
					  - name: "anonymous-1"
					    type: "combinator"
					    combinationKey: "SUM"
					    underlyings:
					    - "k1"
					    - "k2"
					""");

			AdhocBagOfMeasureBag a = fromResource.loadMapFromResource("yaml",
					new ByteArrayResource(amsAsString.getBytes(StandardCharsets.UTF_8)));
			Assertions.assertThat(a.size()).isEqualTo(1);
			Assertions.assertThat(a.getBag("someBagName").getNameToMeasure()).hasSize(4);
		}
	}

	@Test
	public void testBasicFile() throws IOException {
		AdhocBagOfMeasureBag obj = fromResource.loadMapFromResource("yaml", new ClassPathResource("dag_example.yml"));

		Assertions.assertThat(obj.size()).isEqualTo(1);

		AdhocMeasureBag bag = obj.getBag("niceBagName");

		DirectedAcyclicGraph<IMeasure, DefaultEdge> jgrapht = bag.makeMeasuresDag();

		Assertions.assertThat(jgrapht.vertexSet()).hasSize(5);
		Assertions.assertThat(jgrapht.edgeSet()).hasSize(5);
	}

	@Test
	public void testRemoveUselessProperties_Aggregator() {
		ObjectMapper objectMapper = MeasuresSetFromResource.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.k1Sum, Map.class);
		Map<String, ?> cleaned = fromResource.removeUselessProperties(IAdhocTestConstants.k1Sum, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(2)
				.containsEntry("name", "k1")
				.containsEntry("type", "aggregator");
	}

	@Test
	public void testRemoveUselessProperties_Combinator() {
		ObjectMapper objectMapper = MeasuresSetFromResource.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.k1PlusK2AsExpr, Map.class);
		Map<String, ?> cleaned = fromResource.removeUselessProperties(IAdhocTestConstants.k1PlusK2AsExpr, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(5)
				.containsEntry("combinationKey", "EXPRESSION")
				.containsEntry("combinationOptions", Map.of("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)"))
				.containsEntry("name", "k1PlusK2AsExpr")
				.containsEntry("type", "combinator")
				.containsEntry("underlyings", Arrays.asList("k1", "k2"));
	}

	@Test
	public void testRemoveUselessProperties_Bucketor() {
		ObjectMapper objectMapper = MeasuresSetFromResource.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.sum_MaxK1K2ByA, Map.class);
		Map<String, ?> cleaned = fromResource.removeUselessProperties(IAdhocTestConstants.sum_MaxK1K2ByA, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(6)
				.containsEntry("aggregationKey", "SUM")
				.containsEntry("combinationKey", "MAX")
				.containsEntry("groupBy", Set.of("a"))
				.containsEntry("name", "sum_maxK1K2ByA")
				.containsEntry("type", "bucketor")
				.containsEntry("underlyings", Arrays.asList("k1", "k2"));
	}

	@Test
	public void testRemoveUselessProperties_Filtrator() {
		ObjectMapper objectMapper = MeasuresSetFromResource.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.filterK1onA1, Map.class);
		Map<String, ?> cleaned = fromResource.removeUselessProperties(IAdhocTestConstants.filterK1onA1, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(4)
				.containsEntry("filter", ImmutableMap.builder().put("column","a").put("nullIfAbsent",true).put("type","column").put("valueMatcher", Map.of("operand","a1", "type","equals")).build())
				.containsEntry("name", "filterK1onA1")
				.containsEntry("type", "filtrator")
				.containsEntry("underlying", "k1");
	}
}
