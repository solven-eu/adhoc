/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.util;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.pepper.unittest.PepperJackson3TestHelper;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.module.SimpleModule;

public class TestThrowableAsStackSerializer {

	@Test
	public void testSerialization() {
		ObjectMapper objectMapper = PepperJackson3TestHelper.makeObjectMapper();

		SimpleModule adhocModule = new SimpleModule("Adhoc");
		adhocModule.addSerializer(new ThrowableAsStackSerializer());
		objectMapper = objectMapper.rebuild().addModule(adhocModule).build();

		Exception e1 = new IllegalArgumentException("Aie");
		Exception e2 = new UnsupportedOperationException("Ouille", e1);

		String asString = objectMapper.writeValueAsString(e2);
		Assertions.assertThat(asString).contains("Aie", "Ouille");
	}

	@Test
	public void testInMap() {
		ObjectMapper objectMapper = PepperJackson3TestHelper.makeObjectMapper();

		SimpleModule adhocModule = new SimpleModule("Adhoc");
		adhocModule.addSerializer(new ThrowableAsStackSerializer());
		objectMapper = objectMapper.rebuild().addModule(adhocModule).build();

		Exception e1 = new IllegalArgumentException("Aie");
		Exception e2 = new UnsupportedOperationException("Ouille", e1);

		String asString = objectMapper.writeValueAsString(Map.of("e", e2));
		Assertions.assertThat(asString).contains("Aie", "Ouille");
	}
}
