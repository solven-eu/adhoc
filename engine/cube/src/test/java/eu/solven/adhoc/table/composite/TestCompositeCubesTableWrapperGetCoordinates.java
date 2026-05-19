/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.composite;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;

/**
 * Focused unit test for {@link CompositeCubesTableWrapper#getCoordinates(Map, int)}. Pins the bulk-dispatch contract:
 * one bulk call per sub-cube (carrying only that sub-cube's columns), results merged across sub-cubes.
 */
public class TestCompositeCubesTableWrapperGetCoordinates {

	protected ICubeWrapper mockCube(String name, Map<String, ColumnMetadata> columns) {
		ICubeWrapper cube = Mockito.mock(ICubeWrapper.class);
		Mockito.when(cube.getName()).thenReturn(name);
		Mockito.when(cube.getColumnsAsMap()).thenReturn(columns);
		return cube;
	}

	@Test
	public void testBulkDispatch_oneCallPerSubCube() {
		ColumnMetadata countryMeta = ColumnMetadata.builder().name("country").type(String.class).build();
		ColumnMetadata productMeta = ColumnMetadata.builder().name("product").type(String.class).build();

		ICubeWrapper cubeA = mockCube("cubeA", Map.of("country", countryMeta, "product", productMeta));
		ICubeWrapper cubeB = mockCube("cubeB", Map.of("country", countryMeta));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("FR").coordinate("US").estimatedCardinality(2).build(),
						"product",
						CoordinatesSample.builder().coordinate("BOOK").estimatedCardinality(1).build()));
		Mockito.when(cubeB.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("US").coordinate("DE").estimatedCardinality(2).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).cube(cubeB).optCubeSlicer(Optional.empty()).build();

		Map<String, IValueMatcher> request =
				Map.of("country", IValueMatcher.MATCH_ALL, "product", IValueMatcher.MATCH_ALL);

		Map<String, CoordinatesSample> result = composite.getCoordinates(request, 10);

		ArgumentCaptor<Map<String, IValueMatcher>> cubeARequest = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(cubeA, Mockito.times(1)).getCoordinates(cubeARequest.capture(), Mockito.eq(10));
		Assertions.assertThat(cubeARequest.getValue()).containsOnlyKeys("country", "product");

		ArgumentCaptor<Map<String, IValueMatcher>> cubeBRequest = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(cubeB, Mockito.times(1)).getCoordinates(cubeBRequest.capture(), Mockito.eq(10));
		Assertions.assertThat(cubeBRequest.getValue()).containsOnlyKeys("country");

		Assertions.assertThat(result).containsOnlyKeys("country", "product");
		Assertions.assertThat(result.get("country").getCoordinates()).containsExactlyInAnyOrder("FR", "US", "DE");
		// Two sub-cubes each contribute their own estimate; summed even though "US" is shared.
		Assertions.assertThat(result.get("country").getEstimatedCardinality()).isEqualTo(4);
		Assertions.assertThat(result.get("product").getCoordinates()).containsExactly("BOOK");
		Assertions.assertThat(result.get("product").getEstimatedCardinality()).isEqualTo(1);
	}

	@Test
	public void testSubCubeWithNoRequestedColumn_isNotCalled() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));
		ICubeWrapper cubeB =
				mockCube("cubeB", Map.of("region", ColumnMetadata.builder().name("region").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("FR").estimatedCardinality(1).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).cube(cubeB).optCubeSlicer(Optional.empty()).build();

		Map<String, CoordinatesSample> result =
				composite.getCoordinates(Map.of("country", IValueMatcher.MATCH_ALL), 10);

		Mockito.verify(cubeA, Mockito.times(1)).getCoordinates(Mockito.anyMap(), Mockito.eq(10));
		Mockito.verify(cubeB, Mockito.never()).getCoordinates(Mockito.anyMap(), Mockito.anyInt());

		Assertions.assertThat(result).containsOnlyKeys("country");
		Assertions.assertThat(result.get("country").getCoordinates()).containsExactly("FR");
	}

	@Test
	public void testColumnUnknownToAllSubCubes_returnsEmptySample() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("FR").estimatedCardinality(1).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).optCubeSlicer(Optional.empty()).build();

		Map<String, CoordinatesSample> result = composite
				.getCoordinates(Map.of("country", IValueMatcher.MATCH_ALL, "unknown", IValueMatcher.MATCH_ALL), 10);

		Assertions.assertThat(result).containsOnlyKeys("country", "unknown");
		Assertions.assertThat(result.get("unknown").getCoordinates()).isEmpty();
		Assertions.assertThat(result.get("unknown").getEstimatedCardinality())
				.isEqualTo(CoordinatesSample.NO_ESTIMATION);
	}

	@Test
	public void testSlicerColumn_synthesizedLocally() {
		ICubeWrapper cubeA = mockCube("cubeA", Map.of());
		ICubeWrapper cubeB = mockCube("cubeB", Map.of());

		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder()
				.cube(cubeA)
				.cube(cubeB)
				.optCubeSlicer(Optional.of("~CompositeSlicer"))
				.build();

		Map<String, CoordinatesSample> result =
				composite.getCoordinates(Map.of("~CompositeSlicer", IValueMatcher.MATCH_ALL), 10);

		// Sub-cubes are never queried for the slicer column.
		Mockito.verify(cubeA, Mockito.never()).getCoordinates(Mockito.anyMap(), Mockito.anyInt());
		Mockito.verify(cubeB, Mockito.never()).getCoordinates(Mockito.anyMap(), Mockito.anyInt());

		Assertions.assertThat(result).containsOnlyKeys("~CompositeSlicer");
		Assertions.assertThat(result.get("~CompositeSlicer").getCoordinates())
				.containsExactlyInAnyOrder("cubeA", "cubeB");
		Assertions.assertThat(result.get("~CompositeSlicer").getEstimatedCardinality()).isEqualTo(2);
	}

	@Test
	public void testSlicerColumn_honorsValueMatcher() {
		ICubeWrapper cubeA = mockCube("cubeA", Map.of());
		ICubeWrapper cubeB = mockCube("cubeB", Map.of());

		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder()
				.cube(cubeA)
				.cube(cubeB)
				.optCubeSlicer(Optional.of("~CompositeSlicer"))
				.build();

		Map<String, CoordinatesSample> result =
				composite.getCoordinates(Map.of("~CompositeSlicer", EqualsMatcher.matchEq("cubeB")), 10);

		Assertions.assertThat(result.get("~CompositeSlicer").getCoordinates()).containsExactly("cubeB");
		Assertions.assertThat(result.get("~CompositeSlicer").getEstimatedCardinality()).isEqualTo(1);
	}

	@Test
	public void testEmptyRequest_returnsEmptyMap() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).optCubeSlicer(Optional.empty()).build();

		Map<String, CoordinatesSample> result = composite.getCoordinates(Map.of(), 10);

		Assertions.assertThat(result).isEmpty();
		Mockito.verify(cubeA, Mockito.never()).getCoordinates(Mockito.anyMap(), Mockito.anyInt());
	}

	@Test
	public void testMergeSamples_truncatesToLimit() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));
		ICubeWrapper cubeB = mockCube("cubeB",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder()
								.coordinate("a")
								.coordinate("b")
								.coordinate("c")
								.estimatedCardinality(3)
								.build()));
		Mockito.when(cubeB.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("d").coordinate("e").estimatedCardinality(2).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).cube(cubeB).optCubeSlicer(Optional.empty()).build();

		Map<String, CoordinatesSample> result = composite.getCoordinates(Map.of("country", IValueMatcher.MATCH_ALL), 4);

		Assertions.assertThat(result.get("country").getCoordinates()).hasSize(4);
		Assertions.assertThat(result.get("country").getEstimatedCardinality()).isEqualTo(5);
	}

	@Test
	public void testMergeSamples_preservesEstimateWhenOneSideIsUnknown() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));
		ICubeWrapper cubeB = mockCube("cubeB",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country", CoordinatesSample.builder().coordinate("a").build()));
		Mockito.when(cubeB.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(
						Map.of("country", CoordinatesSample.builder().coordinate("b").estimatedCardinality(5).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).cube(cubeB).optCubeSlicer(Optional.empty()).build();

		Map<String, CoordinatesSample> result =
				composite.getCoordinates(Map.of("country", IValueMatcher.MATCH_ALL), 10);

		Assertions.assertThat(result.get("country").getCoordinates()).containsExactlyInAnyOrder("a", "b");
		Assertions.assertThat(result.get("country").getEstimatedCardinality()).isEqualTo(5);
	}

	@Test
	public void testColumnInOneCubeOnly_isDispatchedOnlyToThatCube() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country",
						ColumnMetadata.builder().name("country").type(String.class).build(),
						"product",
						ColumnMetadata.builder().name("product").type(String.class).build()));
		ICubeWrapper cubeB = mockCube("cubeB",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("FR").estimatedCardinality(1).build(),
						"product",
						CoordinatesSample.builder().coordinate("BOOK").estimatedCardinality(1).build()));
		Mockito.when(cubeB.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("DE").estimatedCardinality(1).build()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).cube(cubeB).optCubeSlicer(Optional.empty()).build();

		ArgumentCaptor<Map<String, IValueMatcher>> cubeBRequest = ArgumentCaptor.forClass(Map.class);
		composite.getCoordinates(Map.of("country", IValueMatcher.MATCH_ALL, "product", IValueMatcher.MATCH_ALL), 10);

		Mockito.verify(cubeB).getCoordinates(cubeBRequest.capture(), Mockito.eq(10));
		Assertions.assertThat(cubeBRequest.getValue()).containsOnlyKeys("country").doesNotContainKey("product");
	}

	@Test
	public void testBulkAndSlicer_dispatchedInSingleCall() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));
		ICubeWrapper cubeB = mockCube("cubeB",
				Map.of("country", ColumnMetadata.builder().name("country").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("FR").estimatedCardinality(1).build()));
		Mockito.when(cubeB.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.builder().coordinate("DE").estimatedCardinality(1).build()));

		CompositeCubesTableWrapper composite = CompositeCubesTableWrapper.builder()
				.cube(cubeA)
				.cube(cubeB)
				.optCubeSlicer(Optional.of("~CompositeSlicer"))
				.build();

		Map<String, CoordinatesSample> result = composite.getCoordinates(
				Map.of("country", IValueMatcher.MATCH_ALL, "~CompositeSlicer", IValueMatcher.MATCH_ALL),
				10);

		ArgumentCaptor<Map<String, IValueMatcher>> cubeARequest = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(cubeA).getCoordinates(cubeARequest.capture(), Mockito.eq(10));
		Assertions.assertThat(cubeARequest.getValue()).containsOnlyKeys("country");

		Assertions.assertThat(result).containsOnlyKeys("country", "~CompositeSlicer");
		Assertions.assertThat(result.get("~CompositeSlicer").getCoordinates())
				.containsExactlyInAnyOrder("cubeA", "cubeB");
	}

	@Test
	public void testCubes_callsOnlyOnceEvenWithMultipleColumns() {
		ICubeWrapper cubeA = mockCube("cubeA",
				Map.of("country",
						ColumnMetadata.builder().name("country").type(String.class).build(),
						"product",
						ColumnMetadata.builder().name("product").type(String.class).build(),
						"category",
						ColumnMetadata.builder().name("category").type(String.class).build()));

		Mockito.when(cubeA.getCoordinates(Mockito.anyMap(), Mockito.anyInt()))
				.thenReturn(Map.of("country",
						CoordinatesSample.empty(),
						"product",
						CoordinatesSample.empty(),
						"category",
						CoordinatesSample.empty()));

		CompositeCubesTableWrapper composite =
				CompositeCubesTableWrapper.builder().cube(cubeA).optCubeSlicer(Optional.empty()).build();

		composite.getCoordinates(Map.of("country",
				IValueMatcher.MATCH_ALL,
				"product",
				IValueMatcher.MATCH_ALL,
				"category",
				IValueMatcher.MATCH_ALL), 10);

		// The whole point of overriding: ONE bulk call per sub-cube, not one per column.
		Mockito.verify(cubeA, Mockito.times(1)).getCoordinates(Mockito.anyMap(), Mockito.eq(10));
	}

	// Suppresses the test-only "unused" lint on the list field, kept for parity with the other tests that build lists.
	@SuppressWarnings("unused")
	private List<ICubeWrapper> twoCubes(ICubeWrapper a, ICubeWrapper b) {
		return List.of(a, b);
	}
}
