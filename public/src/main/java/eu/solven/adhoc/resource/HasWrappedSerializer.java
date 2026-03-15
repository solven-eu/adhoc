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

import eu.solven.adhoc.util.IHasWrapped;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * This is used to print a complex `(a -> ref(a), b -> ref(b)` into a simpler `[a, b]`
 * 
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/jackson-custom-serialization
public class HasWrappedSerializer extends StdSerializer<IHasWrapped> {
	public HasWrappedSerializer() {
		this(null);
	}

	public HasWrappedSerializer(Class<IHasWrapped> t) {
		super(t);
	}

	@Override
	public void serializeWithType(IHasWrapped value,
			JsonGenerator gen,
			SerializationContext ctxt,
			TypeSerializer typeSer) {
		gen.writePOJO(value.getWrapped());
	}

	@Override
	public void serialize(IHasWrapped value, JsonGenerator gen, SerializationContext provider) {
		gen.writePOJO(value.getWrapped());
	}

}