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

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.jackson.SliceFilterDeserializer;
import eu.solven.adhoc.query.filter.jackson.SliceFilterSerializer;
import lombok.experimental.UtilityClass;
import tools.jackson.core.util.DefaultIndenter;
import tools.jackson.core.util.DefaultPrettyPrinter;
import tools.jackson.databind.BeanDescription.Supplier;
import tools.jackson.databind.DeserializationConfig;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationConfig;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.deser.ValueDeserializerModifier;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.ValueSerializerModifier;

/**
 * Jackson specific classes for Adhoc public classes.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class AdhocPublicJackson {
	// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
	protected static class SliceFilterSerializerModifier extends ValueSerializerModifier {
		private static final long serialVersionUID = -9176152914317924040L;

		@Override
		public ValueSerializer<?> modifySerializer(SerializationConfig config,
				Supplier beanDesc,
				ValueSerializer<?> serializer) {
			if (ISliceFilter.class.isAssignableFrom(beanDesc.getBeanClass())) {
				return new SliceFilterSerializer((ValueSerializer) serializer);
			}

			return serializer;
		}
	}

	// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
	protected static class SliceFilterDeserializerModifier extends ValueDeserializerModifier {
		private static final long serialVersionUID = -9176152914317924040L;

		@Override
		public ValueDeserializer<?> modifyDeserializer(DeserializationConfig config,
				Supplier beanDescRef,
				ValueDeserializer<?> deserializer) {
			if (ISliceFilter.class.isAssignableFrom(beanDescRef.getBeanClass())) {
				return new SliceFilterDeserializer(deserializer);
			} else {
				return super.modifyDeserializer(config, beanDescRef, deserializer);
			}
		}
	}

	public static SimpleModule makeModule() {
		SimpleModule adhocModule = new SimpleModule("AdhocModule");

		adhocModule.setSerializerModifier(new SliceFilterSerializerModifier());
		adhocModule.setDeserializerModifier(new SliceFilterDeserializerModifier());

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
		return objectMapper.rebuild().defaultPrettyPrinter(prettyPrinter).build();
	}

	public static ObjectMapper makeObjectMapper() {
		return JsonMapper.builder()
				// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
				.enable(SerializationFeature.INDENT_OUTPUT)
				// https://github.com/FasterXML/jackson-databind/issues/5704
				// `@JsonPropertyOrder(alphabetic = false)` is not functional
				.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
				.build();
	}
}
