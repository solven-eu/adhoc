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
package eu.solven.adhoc.column;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import eu.solven.adhoc.util.IHasName;

/**
 * This is used to print a complex `(a -> ref(a), b -> ref(b)` into a simpler `[a, b]`
 * 
 * @author Benoit Lacelle
 */
// https://www.baeldung.com/jackson-custom-serialization
public class NameSerializer extends StdSerializer<IHasName> {
	private static final long serialVersionUID = 1L;

	public NameSerializer() {
		this(null);
	}

	public NameSerializer(Class<IHasName> t) {
		super(t);
	}

	@Override
	public void serializeWithType(IHasName hasName,
			JsonGenerator gen,
			SerializerProvider serializers,
			TypeSerializer typeSer) throws IOException {
		gen.writeString(hasName.getName());
	}

	@Override
	public void serialize(IHasName hasName, JsonGenerator gen, SerializerProvider provider) throws IOException {
		gen.writeString(hasName.getName());
	}

}