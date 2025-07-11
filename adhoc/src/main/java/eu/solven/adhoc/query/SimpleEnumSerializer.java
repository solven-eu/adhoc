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
package eu.solven.adhoc.query;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * This is used to print a complex `(a -> ref(a), b -> ref(b)` into a simpler `[a, b]`
 * 
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/jackson-custom-serialization
public class SimpleEnumSerializer extends StdSerializer<IQueryOption> {
	private static final long serialVersionUID = 1L;

	public SimpleEnumSerializer() {
		this(null);
	}

	public SimpleEnumSerializer(Class<IQueryOption> t) {
		super(t);
	}

	@Override
	public void serializeWithType(IQueryOption value,
			JsonGenerator gen,
			SerializerProvider serializers,
			TypeSerializer typeSer) throws IOException {
		gen.writeObject(value.toString());
	}

	@Override
	public void serialize(IQueryOption value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		gen.writeObject(value.toString());
	}

}