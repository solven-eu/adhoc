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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;

import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.resource.AdhocPublicJackson;

/**
 * {@link Customizer} serialization to write matchAll and matchNone as plain {@link String}.
 * 
 * @author Benoit Lacelle
 */
// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
public class AdhocFilterDeserializer extends JsonDeserializer<IAdhocFilter> implements ResolvableDeserializer {
	// private static final long serialVersionUID = 8174515895932210350L;

	private final JsonDeserializer<?> base;

	public AdhocFilterDeserializer(JsonDeserializer<?> base) {
		this.base = Objects.requireNonNull(base);
	}

	// Used before AdhocFilterDeserializerModifier rewrap it
	public AdhocFilterDeserializer() {
		this.base = null;
	}

	protected IAdhocFilter onText(JsonParser p) throws IOException {
		if ("matchAll".equalsIgnoreCase(p.getText())) {
			return IAdhocFilter.MATCH_ALL;
		} else if ("matchNone".equalsIgnoreCase(p.getText())) {
			return IAdhocFilter.MATCH_NONE;
		} else {
			throw new IllegalArgumentException("Not managed text: %s".formatted(p.getText()));
		}
	}

	@Override
	public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
			throws IOException, JacksonException {
		if (p.hasTextCharacters()) {
			return onText(p);
		} else if (base == null) {
			throw new IllegalStateException(
					"You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			return base.deserializeWithType(p, ctxt, typeDeserializer);
		}
	}

	@Override
	public IAdhocFilter deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		if (p.hasTextCharacters()) {
			return onText(p);
		} else if (base == null) {
			throw new IllegalStateException(
					"You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			return (IAdhocFilter) base.deserialize(p, ctxt);
		}
	}

	@Override
	public void resolve(DeserializationContext ctxt) throws JsonMappingException {
		if (base instanceof ResolvableDeserializer resolvable) {
			resolvable.resolve(ctxt);
		}
	}

}