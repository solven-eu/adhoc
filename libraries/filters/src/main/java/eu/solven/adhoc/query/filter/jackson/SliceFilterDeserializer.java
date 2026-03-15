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

import java.util.Objects;

import org.jspecify.annotations.NonNull;

import eu.solven.adhoc.query.filter.AdhocPublicJackson.SliceFilterDeserializerModifier;
import eu.solven.adhoc.query.filter.ISliceFilter;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.jsontype.TypeDeserializer;

/**
 * Customize serialization to write `matchAll` and `matchNone` as plain {@link String}.
 * 
 * @author Benoit Lacelle
 * @see SliceFilterDeserializerModifier
 */
// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
public class SliceFilterDeserializer extends ValueDeserializer<ISliceFilter> {
	@NonNull
	private final ValueDeserializer<?> base;

	public SliceFilterDeserializer(ValueDeserializer<?> base) {
		this.base = Objects.requireNonNull(base);
	}

	@Override
	public void resolve(DeserializationContext ctxt) {
		super.resolve(ctxt);
		base.resolve(ctxt);
	}

	protected ISliceFilter onString(JsonParser p) {
		String asString = p.getString();
		if ("matchAll".equalsIgnoreCase(asString)) {
			return ISliceFilter.MATCH_ALL;
		} else if ("matchNone".equalsIgnoreCase(asString)) {
			return ISliceFilter.MATCH_NONE;
		} else {
			throw new IllegalArgumentException("Not managed text: %s".formatted(asString));
		}
	}

	@Override
	public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) {
		if (p.hasStringCharacters()) {
			return onString(p);
		} else {
			return (ISliceFilter) base.deserializeWithType(p, ctxt, typeDeserializer);
		}
	}

	@Override
	public ISliceFilter deserialize(JsonParser p, DeserializationContext ctxt) {
		if (p.hasStringCharacters()) {
			return onString(p);
		} else {
			return (ISliceFilter) base.deserialize(p, ctxt);
		}
	}

}