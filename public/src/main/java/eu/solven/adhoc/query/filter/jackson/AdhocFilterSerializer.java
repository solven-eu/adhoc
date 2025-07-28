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
package eu.solven.adhoc.query.filter.jackson;

import java.beans.Customizer;
import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.ResolvableSerializer;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.resource.AdhocPublicJackson;

/**
 * {@link Customizer} serialization to write matchAll and matchNone as plain {@link String}.
 * 
 * @author Benoit Lacelle
 */
// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
public class AdhocFilterSerializer extends JsonSerializer<IAdhocFilter> implements ResolvableSerializer {
	private final JsonSerializer<Object> base;

	public AdhocFilterSerializer(JsonSerializer<Object> base) {
		this.base = Objects.requireNonNull(base);
	}

	// Used before AdhocFilterSerializerModifier rewrap it
	public AdhocFilterSerializer() {
		this.base = null;
	}

	@Override
	public void serializeWithType(IAdhocFilter value,
			JsonGenerator gen,
			SerializerProvider serializers,
			TypeSerializer typeSer) throws IOException {
		if (IAdhocFilter.MATCH_ALL.equals(value)) {
			gen.writeString("matchAll");
		} else if (IAdhocFilter.MATCH_NONE.equals(value)) {
			gen.writeString("matchNone");
		} else if (base == null) {
			throw new IllegalStateException(
					"You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			base.serializeWithType(value, gen, serializers, typeSer);
		}
	}

	@Override
	public void serialize(IAdhocFilter value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		if (IAdhocFilter.MATCH_ALL.equals(value)) {
			gen.writeString("matchAll");
		} else if (IAdhocFilter.MATCH_NONE.equals(value)) {
			gen.writeString("matchNone");
		} else if (base == null) {
			throw new IllegalStateException(
					"You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			base.serialize(value, gen, serializers);
		}
	}

	@Override
	public void resolve(SerializerProvider provider) throws JsonMappingException {
		if (base instanceof ResolvableSerializer resolvable) {
			resolvable.resolve(provider);
		}
	}

}