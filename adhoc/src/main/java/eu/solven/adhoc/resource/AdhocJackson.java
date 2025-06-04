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
package eu.solven.adhoc.resource;

import java.lang.reflect.Method;

import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import lombok.experimental.UtilityClass;

/**
 * Jackson specific classes for Adhoc classes.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocJackson {

	/**
	 * 
	 * @param format
	 *            `json` or `yaml`
	 * @return an {@link ObjectMapper} configured properly for Adhoc classes
	 */
	public static ObjectMapper makeObjectMapper(String format) {
		ObjectMapper objectMapper;
		if ("yml".equalsIgnoreCase(format) || "yaml".equalsIgnoreCase(format)) {
			String yamlFactoryClass = "com.fasterxml.jackson.dataformat.yaml.YAMLFactory";
			if (!ClassUtils.isPresent(yamlFactoryClass, null)) {
				// Adhoc has optional=true, as only a minority of projects uses this library
				throw new IllegalArgumentException(
						"Do you miss an explicit dependency over `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml`?");
			}

			String yamlObjectMapperFactoryClass = "eu.solven.adhoc.resource.AdhocYamlObjectMapper";
			String yamlObjectMapperMethodName = "yamlObjectMapper";
			try {
				Method yamlObjectMapper = ReflectionUtils
						.findMethod(ClassUtils.forName(yamlObjectMapperFactoryClass, null), yamlObjectMapperMethodName);
				if (yamlObjectMapper == null) {
					throw new IllegalStateException("Can not find method %s.%s".formatted(yamlObjectMapperFactoryClass,
							yamlObjectMapperMethodName));
				}
				objectMapper = (ObjectMapper) ReflectionUtils.invokeMethod(yamlObjectMapper, null);
			} catch (ClassNotFoundException e) {
				// This should have been caught preventively
				throw new RuntimeException(e);
			}
		} else {
			objectMapper = new ObjectMapper();
		}

		// We prefer pretty-printing the output
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		return objectMapper;
	}
}
