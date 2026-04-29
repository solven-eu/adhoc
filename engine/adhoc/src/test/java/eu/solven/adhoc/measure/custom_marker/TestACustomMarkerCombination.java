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
package eu.solven.adhoc.measure.custom_marker;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.cuboid.slice.SliceHelpers;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;

public class TestACustomMarkerCombination {

	/** Minimal concrete subclass that reads a single-key path. */
	static class FlatPathCombination extends ACustomMarkerCombination {
		final String path;
		final Object defaultValue;

		FlatPathCombination(String path, Object defaultValue) {
			this.path = path;
			this.defaultValue = defaultValue;
		}

		@Override
		protected String getJoinedMapPath() {
			return path;
		}

		@Override
		protected Object getDefault() {
			return defaultValue;
		}
	}

	private ISliceWithStep sliceWithMarker(Object customMarker) {
		CubeQueryStep step = CubeQueryStep.builder().measure("m").customMarker(customMarker).build();
		return SliceAsMapWithStep.builder().slice(SliceHelpers.asSlice(Map.of())).queryStep(step).build();
	}

	private ISliceWithStep sliceWithoutMarker() {
		CubeQueryStep step = CubeQueryStep.builder().measure("m").build();
		return SliceAsMapWithStep.builder().slice(SliceHelpers.asSlice(Map.of())).queryStep(step).build();
	}

	@Test
	public void testNullCustomMarker_returnsDefault() {
		// Path is never accessed when customMarker is null
		ACustomMarkerCombination combination = new FlatPathCombination("$.myKey", "defaultResult");

		Object result = combination.combine(sliceWithoutMarker(), List.of());

		Assertions.assertThat(result).isEqualTo("defaultResult");
	}

	@Test
	public void testNullCustomMarker_defaultIsNull() {
		ACustomMarkerCombination combination = new FlatPathCombination("$.myKey", null);

		Object result = combination.combine(sliceWithoutMarker(), List.of());

		Assertions.assertThat(result).isNull();
	}

	@Test
	public void testMapCustomMarker_foundViaSplitPath() {
		// "$.myKey" splits to ["myKey"] which matches the flat map key
		ACustomMarkerCombination combination = new FlatPathCombination("$.myKey", "default");

		Object result = combination.combine(sliceWithMarker(Map.of("myKey", 42)), List.of());

		Assertions.assertThat(result).isEqualTo(42);
	}

	@Test
	public void testMapCustomMarker_nestedPath_foundViaSplitPath() {
		// "$.a.b" splits to ["a", "b"] — looks up recursively in nested map
		ACustomMarkerCombination combination = new FlatPathCombination("$.a.b", "default");
		Map<String, Object> nested = Map.of("a", Map.of("b", "deepValue"));

		Object result = combination.combine(sliceWithMarker(nested), List.of());

		Assertions.assertThat(result).isEqualTo("deepValue");
	}

	@Test
	public void testMapCustomMarker_keyNotFound_returnsDefault() {
		ACustomMarkerCombination combination = new FlatPathCombination("$.missingKey", "fallback");

		Object result = combination.combine(sliceWithMarker(Map.of("otherKey", 99)), List.of());

		Assertions.assertThat(result).isEqualTo("fallback");
	}

	@Test
	public void testNonMapCustomMarker_returnsDefault() {
		// When the customMarker is not a Map, the else-branch logs a warning and returns default
		ACustomMarkerCombination combination = new FlatPathCombination("$.myKey", "fallback");

		Object result = combination.combine(sliceWithMarker("notAMap"), List.of());

		Assertions.assertThat(result).isEqualTo("fallback");
	}

	@Test
	public void testMapCustomMarker_flatFallback_foundViaJoinedPath() {
		// "$.a.b" splits to ["a", "b"]; map has no "a" key so recursive lookup fails.
		// The second attempt uses the joined path "$.a.b" as a literal key and finds it.
		ACustomMarkerCombination combination = new FlatPathCombination("$.a.b", "default");
		Map<String, Object> flatMap = Map.of("$.a.b", "flatValue");

		Object result = combination.combine(sliceWithMarker(flatMap), List.of());

		Assertions.assertThat(result).isEqualTo("flatValue");
	}
}
