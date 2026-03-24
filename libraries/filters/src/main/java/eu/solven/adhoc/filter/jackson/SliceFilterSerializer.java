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
package eu.solven.adhoc.filter.jackson;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

import eu.solven.adhoc.filter.AdhocPublicJackson.SliceFilterSerializerModifier;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.SimpleAndFilter;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * Customize serialization to write `matchAll` and `matchNone` as plain {@link String}.
 * 
 * @author Benoit Lacelle
 * @see SliceFilterSerializerModifier
 */
// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
public class SliceFilterSerializer extends StdSerializer<ISliceFilter> {
	@NonNull
	final ValueSerializer<ISliceFilter> base;

	public SliceFilterSerializer(ValueSerializer<ISliceFilter> base) {
		super(ISliceFilter.class);
		this.base = Objects.requireNonNull(base);
	}

	@Override
	public void resolve(SerializationContext ctxt) {
		super.resolve(ctxt);
		base.resolve(ctxt);
	}

	@Override
	public void serializeWithType(ISliceFilter value,
			JsonGenerator gen,
			SerializationContext ctxt,
			TypeSerializer typeSer) {
		if (ISliceFilter.MATCH_ALL.equals(value)) {
			gen.writeString("matchAll");
		} else if (ISliceFilter.MATCH_NONE.equals(value)) {
			gen.writeString("matchNone");
		} else if (value instanceof SimpleAndFilter simpleAnd) {
			// Serialise as a plain AndFilter so the wire format is identical ("type":"and")
			base.serializeWithType(AndFilter.copyOf(simpleAnd.getOperands()), gen, ctxt, typeSer);
		} else {
			base.serializeWithType(value, gen, ctxt, typeSer);
		}
	}

	@Override
	public void serialize(ISliceFilter value, JsonGenerator gen, SerializationContext ctxt) {
		if (ISliceFilter.MATCH_ALL.equals(value)) {
			gen.writeString("matchAll");
		} else if (ISliceFilter.MATCH_NONE.equals(value)) {
			gen.writeString("matchNone");
		} else if (value instanceof SimpleAndFilter simpleAnd) {
			// Serialise as a plain AndFilter so the wire format is identical ("type":"and")
			base.serialize(AndFilter.copyOf(simpleAnd.getOperands()), gen, ctxt);
		} else {
			base.serialize(value, gen, ctxt);
		}
	}

}