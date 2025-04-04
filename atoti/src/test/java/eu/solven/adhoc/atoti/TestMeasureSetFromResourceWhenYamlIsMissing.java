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
package eu.solven.adhoc.atoti;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

import eu.solven.adhoc.resource.MeasureForestFromResource;
import eu.solven.adhoc.resource.MeasureForests;

public class TestMeasureSetFromResourceWhenYamlIsMissing {
	// This is especially useful to ensure our code manages properly when YAMLFactory is not Available
	@Test
	public void testYamlFactory() {
		String yamlFactoryClass = "com.fasterxml.jackson.dataformat.yaml.YAMLFactory";

		// This test is relevant only if YAMLFactory is not available
		Assertions.assertThat(ClassUtils.isPresent(yamlFactoryClass, null)).isFalse();

		MeasureForests emptyBag = MeasureForests.builder().build();

		Assertions.assertThat(new MeasureForestFromResource().asString("json", emptyBag)).isEqualTo("[ ]");

		Assertions
				.assertThatThrownBy(
						() -> Assertions.assertThat(new MeasureForestFromResource().asString("yaml", emptyBag)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml");
	}
}
