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
import java.util.Objects;

import eu.solven.adhoc.query.filter.ISliceFilter;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * {@link Customizer} serialization to write matchAll and matchNone as plain {@link String}.
 * 
 * @author Benoit Lacelle
 */
// https://stackoverflow.com/questions/58963529/custom-serializer-with-fallback-to-default-serialization
public class SliceFilterSerializer extends StdSerializer<ISliceFilter> {
	private final ValueSerializer<ISliceFilter> base;

	public SliceFilterSerializer(ValueSerializer<ISliceFilter> base) {
		super(ISliceFilter.class);
		this.base = Objects.requireNonNull(base);
	}

	// Used before AdhocFilterSerializerModifier rewrap it
	public SliceFilterSerializer() {
		super(ISliceFilter.class);
		this.base = null;
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
			// } else if (base == null) {
			// throw new IllegalStateException(
			// "You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			ValueSerializer<Object> delegate = ctxt.findValueSerializer(value.getClass());
			// if (this == delegate) {
			super.serializeWithType(value, gen, ctxt, typeSer);
			base.serializeWithType(value, gen, ctxt, typeSer);
			// } else {
			delegate.serializeWithType(value, gen, ctxt, typeSer);
			// }
		}
	}

	@Override
	public void serialize(ISliceFilter value, JsonGenerator gen, SerializationContext ctxt) {
		if (ISliceFilter.MATCH_ALL.equals(value)) {
			gen.writeString("matchAll");
		} else if (ISliceFilter.MATCH_NONE.equals(value)) {
			gen.writeString("matchNone");
			// } else if (base == null) {
			// throw new IllegalStateException(
			// "You need to register %s.%s".formatted(AdhocPublicJackson.class.getName(), "makeAdhocModule"));
		} else {
			ValueSerializer<Object> delegate = ctxt.findValueSerializer(value.getClass());
			delegate.serialize(value, gen, ctxt);
			// base.serialize(value, gen, ctxt);
		}
	}

}