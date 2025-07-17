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
package eu.solven.adhoc.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;
import eu.solven.adhoc.measure.transformator.MapWithNulls;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestEvaluatedExpressionColumn {

	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(EvaluatedExpressionColumn.class).verify();
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		EvaluatedExpressionColumn column =
				EvaluatedExpressionColumn.builder().name("someColumn").expression("a + b").build();
		String asString = TestMapBasedTabularView.verifyJackson(IAdhocColumn.class, column);

		Assertions.assertThat(asString).isEqualTo("""
				{
				  "type" : ".EvaluatedExpressionColumn",
				  "name" : "someColumn",
				  "type" : "java.lang.Object",
				  "expression" : "a + b"
				}""");
	}

	@Test
	public void testEvaluate() throws JsonProcessingException {
		EvaluatedExpressionColumn column =
				EvaluatedExpressionColumn.builder().name("someColumn").expression("a + \"-\" + b").build();
		// not null
		Assertions.assertThat(column.computeCoordinate(
				TabularRecordOverMaps.builder().slice(SliceAsMap.fromMap(Map.of("a", "a1", "b", "b1"))).build()))
				.isEqualTo("a1-b1");

		// one is null
		Assertions.assertThat(column.computeCoordinate(TabularRecordOverMaps.builder()
				.slice(SliceAsMap.fromMap(MapWithNulls.of("a", "a1", "b", null)))
				.build())).isEqualTo("a1-null");
	}

	@Test
	public void testEvaluate_missingGroupBy() throws JsonProcessingException {
		EvaluatedExpressionColumn column =
				EvaluatedExpressionColumn.builder().name("someColumn").expression("a + \"-\" + b").build();

		Assertions
				.assertThatThrownBy(() -> column.computeCoordinate(
						TabularRecordOverMaps.builder().slice(SliceAsMap.fromMap(Map.of("a", "a1"))).build()))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
