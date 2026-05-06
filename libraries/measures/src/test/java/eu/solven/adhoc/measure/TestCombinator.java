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
package eu.solven.adhoc.measure;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.lambda.LambdaCombination;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Partitionor;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.pepper.unittest.PepperJackson3TestHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCombinator {
	@Test
	public void testOptions_bucketor() {
		Partitionor measure = Partitionor.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.groupBy(GroupByColumns.named("c"))
				.build();
		Map<String, ?> allOptions = Combinator.makeAllOptions(measure, Map.of("k2", "v2"));

		Assertions.assertThat(Map.<String, Object>copyOf(allOptions))
				.hasSize(4)
				.containsEntry("k2", "v2")
				.containsEntry("underlyingNames", List.of())
				// .containsEntry("groupByColumns", Set.of("c"))
				.containsEntry("measure", measure);

		// This checks there is no StackOverFlow on .toString, which is possible as we may set `measure==this` in
		// `.makeAllOptions`
		Assertions.assertThat(measure.toString()).contains("measure");
	}

	@Test
	public void testWithTags() {
		Combinator measure = Combinator.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.combinationKey("someKey")
				.tag("tag1")
				.build();

		Assertions.assertThat(measure.withTags(ImmutableSet.of("tag2"))).satisfies(edited -> {
			Assertions.assertThat(edited.getTags()).containsExactly("tag2");
		});
	}

	@Test
	public void testToString_minimal() {
		Combinator measure = Combinator.builder().name("m").underlying("u1").build();

		// Empty tags and empty combinationOptions are skipped — keeps EXPLAIN traces and exception messages
		// readable on the common case.
		Assertions.assertThat(measure)
				.hasToString("Combinator(name=m, underlyings=[u1], combinationKey=" + SumCombination.KEY + ")");
	}

	@Test
	public void testToString_allCustomized() {
		Combinator measure = Combinator.builder()
				.name("m")
				.tag("t1")
				.tag("t2")
				.underlying("u1")
				.underlying("u2")
				.combinationKey("someKey")
				.combinationOption("k", "v")
				.build();

		Assertions.assertThat(measure)
				.hasToString(
						"Combinator(name=m, tags=[t1, t2], underlyings=[u1, u2], combinationKey=someKey, combinationOptions={k=v})");
	}

	@Test
	public void testAddOptions() {
		Combinator measure = Combinator.builder()
				.name("measureName")
				.combinationOptions(Map.of("k", "v"))
				.combinationKey("someKey")
				.tag("tag1")
				.build();

		Combinator moreOptions = measure.toBuilder().combinationOption("k2", "v2").build();

		Assertions.assertThat(measure.getCombinationOptions()).isEqualTo(Map.of("k", "v"));
		Assertions.assertThat(moreOptions.getCombinationOptions()).isEqualTo(Map.of("k", "v", "k2", "v2"));
	}

	public static Object lambdaAsMethod(ISliceWithStep slice, List<?> values) {
		return "outputValue";
	}

	public static class NestedClass {
		public static Object lambdaAsMethodInNested(ISliceWithStep slice, List<?> values) {
			return "outputValue";
		}
	}

	@Test
	public void testLambda_Serializable_static() {
		ILambdaCombinationS lambdaCombinationS = TestCombinator::lambdaAsMethod;
		Assertions.assertThat(lambdaToString(lambdaCombinationS))
				.isEqualTo("eu.solven.adhoc.measure.TestCombinator::lambdaAsMethod");

	}

	@Test
	public void testLambda_Serializable_Nested() {
		ILambdaCombinationS lambdaCombinationSNested = NestedClass::lambdaAsMethodInNested;
		Assertions.assertThat(lambdaToString(lambdaCombinationSNested))
				.isEqualTo("eu.solven.adhoc.measure.TestCombinator$NestedClass::lambdaAsMethodInNested");

	}

	@Test
	public void testLambda_Serializable_anonymous() {
		ILambdaCombinationS lambdaCombinationS2 = (_, _) -> "outputValue";
		Assertions.assertThat(lambdaToString(lambdaCombinationS2))
				.isEqualTo("eu.solven.adhoc.measure.TestCombinator::lambda$3");

		Combinator measure = Combinator.builder().name("measureName").lambda(lambdaCombinationS2).build();

		String asString = PepperJackson3TestHelper.asString(measure);

		// BEWARE This demonstrate the Lambda is still not serialized in a nice form
		Assertions.assertThat(asString).isEqualTo("""
				{
				  "type" : ".Combinator",
				  "combinationKey" : "eu.solven.adhoc.measure.lambda.LambdaCombination",
				  "combinationOptions" : {
				    "lambda" : { }
				  },
				  "name" : "measureName",
				  "tags" : [ ],
				  "underlyings" : [ ]
				}""");
	}

	/**
	 * Combines a Lambda with a {@link Serializable} contract, enabling access to {@link SerializedLambda}.
	 */
	@FunctionalInterface
	public interface ILambdaCombinationS extends LambdaCombination.ILambdaCombination, Serializable {

	}

	public static <L extends Serializable> String lambdaToString(L lambda) {
		try {
			Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
			writeReplace.setAccessible(true);
			SerializedLambda serializedLambda = (SerializedLambda) writeReplace.invoke(lambda);

			// `getCapturingClass` returns the root class but not the nested class if any
			String capturingClassWithPackage = serializedLambda.getImplClass().replace('/', '.');
			return capturingClassWithPackage + "::" + serializedLambda.getImplMethodName();
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			log.warn("Issue with SerializedLambda", e);
			return "TODO lambdaToString";
		}
	}
}
