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
import eu.solven.adhoc.measure.MeasureBagTestHelpers;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.filter.ColumnFilter;

//An `implicit` measure is a measure which is not defined by itself, but as an underlying of another measure. It leads to deeper (hence more compact but more complex) trees
//An `anonymous` measure is a measure which has no name. It leads to more compact but less re-usable trees.
public class TestMeasureForestFromResource {
	final MeasureForestFromResource fromResource = new MeasureForestFromResource();

	@Test
	public void testFaultInKey_type() {
		Map<String, Object> input = Map.of("underlyings", List.of("k1", "k2"));

		Assertions.assertThatThrownBy(() -> fromResource.getListParameter(input, "undelryings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Did you mean `underlyings` instead of `undelryings`");
	}

	@Test
	public void testFaultInKey_missing() {
		Map<String, Object> input = Map.of("type", "combinator");

		Assertions.assertThatThrownBy(() -> fromResource.getListParameter(input, "underlyings"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageNotContaining("instead of");
	}

	@Test
	public void testDeepMeasuresAsUnderlyings() {
		Map<String, Object> input = ImmutableMap.<String, Object>builder()
				.put("name", "k")
				.put("type", ".Combinator")
				.put("underlyings",
						List.of(ImmutableMap.<String, Object>builder()
								.put("name", "k1")
								.put("type", ".Combinator")
								.put("underlyings",
										Arrays.asList(
												ImmutableMap.builder()
														.put("name", "k11")
														.put("type", ".Combinator")
														.put("underlyings", Arrays.asList("k111"))
														.build(),
												ImmutableMap.builder()
														.put("name", "k12")
														.put("type", ".Combinator")
														.put("underlyings", Arrays.asList("k121"))
														.build()))
								.build(),
								ImmutableMap.<String, Object>builder()
										.put("name", "k2")
										.put("type", ".Combinator")
										.put("underlyings",
												Arrays.asList(
														ImmutableMap.builder()
																.put("name", "k21")
																.put("type", ".Combinator")
																.put("underlyings", Arrays.asList("k211"))
																.build(),
														ImmutableMap.builder()
																.put("name", "k22")
																.put("type", ".Combinator")
																.put("underlyings", Arrays.asList("k221"))
																.build()))
										.build()))
				.build();

		List<IMeasure> measures = fromResource.makeMeasure(input);
		// TODO: Should these be defaulted?
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k111", "type", ".Aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k121", "type", ".Aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k211", "type", ".Aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k221", "type", ".Aggregator")));

		ListAssert<IMeasure> assertMeasures = Assertions.assertThat(measures).hasSize(11);
		assertMeasures.element(0)
				// The first measure must be the explicit measure
				.satisfies(m -> Assertions.assertThat(m.getName()).isEqualTo("k"));

		MeasureForest ams = MeasureForest.fromMeasures("testDeepMeasuresAsUnderlyings", measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(ams);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(11);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(10);

		Assertions.assertThat(ams.resolveIfRef(ReferencedMeasure.builder().ref("k12").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k121");
				});
	}

	// @Disabled("The actual feature+usecase is unclear")
	@Test
	public void testAnonymousUnderlyingNode() throws IOException {
		Map<String, Object> input = ImmutableMap.<String, Object>builder()
				.put("name", "k1Byk1k2")
				.put("type", ".Combinator")
				.put("combinationKey", "DIVIDE")
				.put("underlyings",
						List.of("k1",
								// The denominator definition is anonymous: it does not have as explicit name
								ImmutableMap.builder()
										.put("type", ".Combinator")
										.put("underlyings", List.of("k1", "k2"))
										.build()))
				.build();

		List<IMeasure> measures = fromResource.makeMeasure(input);
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k1", "type", ".Aggregator")));
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k2", "type", ".Aggregator")));

		ListAssert<IMeasure> assertMeasures = Assertions.assertThat(measures).hasSize(4);
		assertMeasures.element(0)
				// The first measure must be the explicit measure
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getName()).isEqualTo("k1Byk1k2");
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1", "anonymous-0");
				});

		MeasureForest forest = MeasureForest.fromMeasures("testAnonymousUnderlyingNode", measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(forest);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(4);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(4);

		Assertions.assertThat(forest.resolveIfRef(ReferencedMeasure.builder().ref("k1Byk1k2").build()))
				.isInstanceOfSatisfying(Combinator.class, c -> {
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1", "anonymous-0");
				});

		{
			MeasureForests bagOfBag = MeasureForests.builder().forest(forest).build();

			String amsAsString = fromResource.asString("yml", bagOfBag);
			Assertions.assertThat(amsAsString).isEqualTo("""
					- name: "testAnonymousUnderlyingNode"
					  measures:
					  - name: "anonymous-0"
					    type: ".Combinator"
					    combinationKey: "SUM"
					    underlyings:
					    - "k1"
					    - "k2"
					  - name: "k1"
					    type: ".Aggregator"
					  - name: "k1Byk1k2"
					    type: ".Combinator"
					    combinationKey: "DIVIDE"
					    underlyings:
					    - "k1"
					    - "anonymous-0"
					  - name: "k2"
					    type: ".Aggregator"
										""");

			MeasureForests a = fromResource.loadMapFromResource("yaml",
					new ByteArrayResource(amsAsString.getBytes(StandardCharsets.UTF_8)));
			Assertions.assertThat(a.size()).isEqualTo(1);
			Assertions.assertThat(a.getForest("testAnonymousUnderlyingNode").getNameToMeasure()).hasSize(4);
		}
	}

	@Test
	public void testFiltrator() throws IOException {
		Map<String, Object> input = ImmutableMap.<String, Object>builder()
				.put("name", "k1_c=V")
				.put("type", ".Filtrator")
				.put("filter",
						Map.of("type",
								"column",
								"column",
								"c",
								"valueMatcher",
								Map.of("type", "equals", "operand", "someString")))
				.put("underlying", "k1")
				.build();

		List<IMeasure> measures = fromResource.makeMeasure(input);
		measures.addAll(fromResource.makeMeasure(Map.of("name", "k1", "type", ".Aggregator")));

		ListAssert<IMeasure> assertMeasures = Assertions.assertThat(measures).hasSize(2);
		assertMeasures.element(0)
				// The first measure must be the explicit measure
				.isInstanceOfSatisfying(Filtrator.class, c -> {
					Assertions.assertThat(c.getName()).isEqualTo("k1_c=V");
					Assertions.assertThat(c.getUnderlyingNames()).containsExactly("k1");
					Assertions.assertThat(c.getFilter()).isEqualTo(ColumnFilter.isEqualTo("c", "someString"));
				});

		MeasureForest forest = MeasureForest.fromMeasures("testWithFilter", measures);

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(forest);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(2);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(1);

		{
			MeasureForests bagOfBag = MeasureForests.builder().forest(forest).build();

			String amsAsString = fromResource.asString("yml", bagOfBag);
			Assertions.assertThat(amsAsString).isEqualTo("""
					- name: "testWithFilter"
					  measures:
					  - name: "k1"
					    type: ".Aggregator"
					  - name: "k1_c=V"
					    type: ".Filtrator"
					    filter:
					      type: "column"
					      column: "c"
					      valueMatcher: "someString"
					      nullIfAbsent: true
					    underlying: "k1"
					""");

			MeasureForests a = fromResource.loadMapFromResource("yaml",
					new ByteArrayResource(amsAsString.getBytes(StandardCharsets.UTF_8)));
			Assertions.assertThat(a.size()).isEqualTo(1);
			Assertions.assertThat(a.getForest("testWithFilter").getNameToMeasure()).hasSize(2);
		}
	}

	@Test
	public void testUnfiltrator() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testUnfiltrator").build();

		measureBag.addMeasure(IAdhocTestConstants.unfilterOnA);
		measureBag.addMeasure(IAdhocTestConstants.k1Sum);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testUnfiltrator",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(2);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(1);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "k1",
				  "type" : ".Aggregator"
				}, {
				  "name" : "unfilterOnK1",
				  "type" : ".Unfiltrator",
				  "inverse" : false,
				  "underlying" : "k1",
				  "unfiltereds" : [ "a" ]
				} ]
				""".strip());
	}

	@Test
	public void testShiftor() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testShiftor").build();

		measureBag.addMeasure(IAdhocTestConstants.shiftorAisA1);
		measureBag.addMeasure(IAdhocTestConstants.k1Sum);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testShiftor",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(2);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(1);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "shiftorAisA1",
				  "type" : ".Shiftor",
				  "editorKey" : "simple",
				  "editorOptions" : {
				    "shifted" : {
				      "a" : "a1"
				    }
				  },
				  "underlying" : "k1"
				}, {
				  "name" : "k1",
				  "type" : ".Aggregator"
				} ]
								                """.strip());
	}

	@Test
	public void testBucketor() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testBucketor").build();

		measureBag.addMeasure(IAdhocTestConstants.sum_MaxK1K2ByA);
		measureBag.addMeasure(IAdhocTestConstants.k1Sum);
		measureBag.addMeasure(IAdhocTestConstants.k2Sum);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testBucketor",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(3);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(2);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "sum_maxK1K2ByA",
				  "type" : ".Bucketor",
				  "aggregationKey" : "SUM",
				  "combinationKey" : "MAX",
				  "groupBy" : {
				    "columns" : [ "a" ]
				  },
				  "underlyings" : [ "k1", "k2" ]
				}, {
				  "name" : "k1",
				  "type" : ".Aggregator"
				}, {
				  "name" : "k2",
				  "type" : ".Aggregator"
				} ]
				""".strip());
	}

	@Test
	public void testDispatchor() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testDispatchor").build();

		measureBag.addMeasure(IAdhocTestConstants.dispatchFrom0To100);
		measureBag.addMeasure(IAdhocTestConstants.k1Sum);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testDispatchor",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(2);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(1);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "k1",
				  "type" : ".Aggregator"
				}, {
				  "name" : "0or100",
				  "type" : ".Dispatchor",
				  "aggregationKey" : "SUM",
				  "decompositionKey" : "linear",
				  "decompositionOptions" : {
				    "input" : "percent",
				    "min" : 0,
				    "max" : 100,
				    "output" : "0_or_100"
				  },
				  "underlying" : "k1"
				} ]
				""".strip());
	}

	@Test
	public void testCustomMeasure() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testCustomMeasure").build();

		measureBag.addMeasure(
				CustomMeasureForResource.builder().name("someCustomName").customProperty("customValue").build());
		measureBag.addMeasure(IAdhocTestConstants.k1Sum);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testCustomMeasure",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(2);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(0);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "k1",
				  "type" : ".Aggregator"
				}, {
				  "name" : "someCustomName",
				  "type" : "eu.solven.adhoc.resource.CustomMeasureForResource"
				} ]
				""".strip());
	}

	@Test
	public void testAggregator_countAsterisk() throws IOException {
		UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder().name("testAggregator_countAsterisk").build();

		measureBag.addMeasure(IAdhocTestConstants.countAsterisk);

		String asString = fromResource.asString("json", measureBag);
		MeasureForest fromString = fromResource.loadForestFromResource("testAggregator_countAsterisk",
				"json",
				new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = MeasureBagTestHelpers.makeMeasuresDag(fromString);
		Assertions.assertThat(measuresDag.vertexSet()).hasSize(1);
		Assertions.assertThat(measuresDag.edgeSet()).hasSize(0);

		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				[ {
				  "name" : "count(*)",
				  "type" : ".Aggregator",
				  "aggregationKey" : "COUNT",
				  "columnName" : "*"
				} ]
				""".strip());
	}

	@Test
	public void testBasicFile() throws IOException {
		MeasureForests obj = fromResource.loadMapFromResource("yaml", new ClassPathResource("dag_example.yml"));

		Assertions.assertThat(obj.size()).isEqualTo(1);

		MeasureForest bag = (MeasureForest) obj.getForest("niceBagName");

		DirectedAcyclicGraph<IMeasure, DefaultEdge> jgrapht = MeasureBagTestHelpers.makeMeasuresDag(bag);

		Assertions.assertThat(jgrapht.vertexSet()).hasSize(5);
		Assertions.assertThat(jgrapht.edgeSet()).hasSize(5);
	}

	@Test
	public void testRemoveUselessProperties_Aggregator() {
		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.k1Sum, Map.class);
		Map<String, ?> cleaned = fromResource.simplifyProperties(IAdhocTestConstants.k1Sum, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(2)
				.containsEntry("name", "k1")
				.containsEntry("type", ".Aggregator");
	}

	@Test
	public void testRemoveUselessProperties_Aggregator_differentColumnName() throws IOException {
		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");
		Aggregator measure = Aggregator.edit(IAdhocTestConstants.k1Sum).columnName("legacyColumnName").build();

		Map<String, ?> rawMap = objectMapper.convertValue(measure, Map.class);
		Map<String, ?> cleaned = fromResource.simplifyProperties(measure, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(3)
				.containsEntry("name", "k1")
				.containsEntry("type", ".Aggregator")
				.containsEntry("columnName", "legacyColumnName");

		{
			UnsafeMeasureForest measureBag = UnsafeMeasureForest.builder()
					.name("testRemoveUselessProperties_Aggregator_differentColumnName")
					.build();

			measureBag.addMeasure(measure);

			String asString = fromResource.asString("json", measureBag);
			MeasureForest fromString =
					fromResource.loadForestFromResource("testRemoveUselessProperties_Aggregator_differentColumnName",
							"json",
							new ByteArrayResource(asString.getBytes(StandardCharsets.UTF_8)));

			Assertions.assertThat(fromString.getNameToMeasure().get("k1")).isEqualTo(measure);
		}
	}

	@Test
	public void testRemoveUselessProperties_Combinator() {
		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.k1PlusK2AsExpr, Map.class);
		Map<String, ?> cleaned = fromResource.simplifyProperties(IAdhocTestConstants.k1PlusK2AsExpr, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(5)
				.containsEntry("combinationKey", "EXPRESSION")
				.containsEntry("combinationOptions",
						Map.of("expression", "IF(k1 == null, 0, k1) + IF(k2 == null, 0, k2)"))
				.containsEntry("name", "k1PlusK2AsExpr")
				.containsEntry("type", ".Combinator")
				.containsEntry("underlyings", Arrays.asList("k1", "k2"));
	}

	@Test
	public void testRemoveUselessProperties_Bucketor() {
		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.sum_MaxK1K2ByA, Map.class);
		Map<String, ?> cleaned = fromResource.simplifyProperties(IAdhocTestConstants.sum_MaxK1K2ByA, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(6)
				.containsEntry("aggregationKey", "SUM")
				.containsEntry("combinationKey", "MAX")
				.containsEntry("groupBy", Map.of("columns", List.of("a")))
				.containsEntry("name", "sum_maxK1K2ByA")
				.containsEntry("type", ".Bucketor")
				.containsEntry("underlyings", Arrays.asList("k1", "k2"));
	}

	@Test
	public void testRemoveUselessProperties_Filtrator() {
		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");

		Map<String, ?> rawMap = objectMapper.convertValue(IAdhocTestConstants.filterK1onA1, Map.class);
		Map<String, ?> cleaned = fromResource.simplifyProperties(IAdhocTestConstants.filterK1onA1, rawMap);

		Assertions.assertThat((Map) cleaned)
				.hasSize(4)
				.containsEntry("filter",
						ImmutableMap.builder()
								.put("column", "a")
								.put("nullIfAbsent", true)
								.put("type", "column")
								.put("valueMatcher", "a1")
								.build())
				.containsEntry("name", "filterK1onA1")
				.containsEntry("type", ".Filtrator")
				.containsEntry("underlying", "k1");
	}
}
