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

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.jackson.AdhocFilterDeserializer;
import eu.solven.adhoc.query.filter.jackson.AdhocFilterSerializer;
import lombok.experimental.UtilityClass;

/**
 * Jackson specific classes for Adhoc public classes.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocPublicJackson {
	// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
	protected static class AdhocFilterSerializerModifier extends BeanSerializerModifier {
		private static final long serialVersionUID = -9176152914317924040L;

		@Override
		public JsonSerializer<?> modifySerializer(SerializationConfig config,
				BeanDescription beanDesc,
				JsonSerializer<?> serializer) {
			if (ISliceFilter.class.isAssignableFrom(beanDesc.getBeanClass())) {
				return new AdhocFilterSerializer((JsonSerializer) serializer);
			}

			return serializer;
		}
	}

	// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
	protected static class AdhocFilterDeserializerModifier extends BeanDeserializerModifier {
		private static final long serialVersionUID = -9176152914317924040L;

		@Override
		public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config,
				BeanDescription beanDesc,
				JsonDeserializer<?> deserializer) {

			if (ISliceFilter.class.isAssignableFrom(beanDesc.getBeanClass())) {
				return new AdhocFilterDeserializer(deserializer);
			} else {
				return super.modifyDeserializer(config, beanDesc, deserializer);
			}
		}
	}

	public static SimpleModule makeModule() {
		SimpleModule adhocModule = new SimpleModule("AdhocModule");

		adhocModule.setSerializerModifier(new AdhocFilterSerializerModifier());
		adhocModule.setDeserializerModifier(new AdhocFilterDeserializerModifier());

		return adhocModule;
	}

	/**
	 * This will change the output format, printing arrays with one element per row.
	 * 
	 * @param objectMapper
	 * @return the input {@link ObjectMapper}, for convenience.
	 */
	public static ObjectMapper indentArrayWithEol(ObjectMapper objectMapper) {
		// https://stackoverflow.com/questions/14938667/jackson-json-deserialization-array-elements-in-each-line
		DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
		prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
		objectMapper.setDefaultPrettyPrinter(prettyPrinter);

		return objectMapper;
	}
}
