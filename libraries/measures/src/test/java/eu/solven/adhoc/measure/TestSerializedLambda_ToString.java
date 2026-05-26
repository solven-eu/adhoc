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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.lambda.LambdaCombination;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.pepper.unittest.PepperJackson3TestHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestSerializedLambda_ToString {

	public static Object lambdaAsMethod(ISliceWithStep slice, List<?> values) {
		return "outputValue";
	}

	public static class NestedClass {
		public static Object lambdaAsMethodInNested(ISliceWithStep slice, List<?> values) {
			return "outputValue";
		}

		public Object lambdaAsMethodInNestedNotStatic(ISliceWithStep slice, List<?> values) {
			return "outputValueNotStatic";
		}

		@Override
		public String toString() {
			return "theNestedInstance";
		}
	}

	@Test
	public void testLambda_Serializable_static() {
		ILambdaCombinationS lambdaCombinationS = TestSerializedLambda_ToString::lambdaAsMethod;
		Assertions.assertThat(lambdaToString(lambdaCombinationS))
				.isEqualTo("eu.solven.adhoc.measure.TestSerializedLambda_ToString::lambdaAsMethod");

	}

	@Test
	public void testLambda_Serializable_Nested() {
		ILambdaCombinationS lambdaCombinationSNested = NestedClass::lambdaAsMethodInNested;
		Assertions.assertThat(lambdaToString(lambdaCombinationSNested))
				.isEqualTo("eu.solven.adhoc.measure.TestSerializedLambda_ToString$NestedClass::lambdaAsMethodInNested");

	}

	@Disabled("Different behaviors in IDE and in CLI")
	@Test
	public void testLambda_Serializable_anonymous() {
		ILambdaCombinationS lambdaCombinationS2 = (_, _) -> "outputValue";
		Assertions.assertThat(lambdaToString(lambdaCombinationS2))
				.isEqualTo("eu.solven.adhoc.measure.TestSerializedLambda_ToString::lambda$2");

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

	@Test
	public void testLambda_Serializable_boundInstanceRef() {
		NestedClass instance = new NestedClass();
		ILambdaCombinationS lambda = instance::lambdaAsMethodInNestedNotStatic;
		Assertions.assertThat(lambdaToString(lambda))
				.isEqualTo(
						"eu.solven.adhoc.measure.TestSerializedLambda_ToString$NestedClass::lambdaAsMethodInNested(theNestedInstance)");
	}

	@Test
	public void testLambda_Serializable_capturedArgs() {
		String capturedStr = "hello";
		int capturedInt = 42;
		ILambdaCombinationS lambda = (slice, values) -> capturedStr + capturedInt;
		// Method name varies between IDE and CLI (lambda index), so we use a pattern match
		Assertions.assertThat(lambdaToString(lambda))
				.matches(
						"eu\\.solven\\.adhoc\\.measure\\.TestSerializedLambda_ToString::lambda\\$.*\\(hello,42\\)");
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
