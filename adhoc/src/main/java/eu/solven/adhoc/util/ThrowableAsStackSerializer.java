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
package eu.solven.adhoc.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Throwables;

/**
 * Serialize a {@link Throwable} by its {@link Throwables#getStackTraceAsString(Throwable)} as a single String.
 * 
 * @author Benoit Lacelle
 */
// TODO Study what's available with SpringBoot, especially with the optional StackTrace.
public class ThrowableAsStackSerializer extends StdSerializer<Throwable> {
	private static final long serialVersionUID = 6220926552609493941L;

	public ThrowableAsStackSerializer() {
		super(Throwable.class);
	}

	@Override
	public void serialize(Throwable value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
		gen.writeStringField("class_name", value.getClass().getName());
		gen.writeStringField("message", value.getMessage());

		String stackTraceAsString = Throwables.getStackTraceAsString(value);

		// Ensure the stack-trace is easy to read for a human
		stackTraceAsString = stackTraceAsString.replaceAll("\t", "    ");
		// Split by EOL, to ensure have nice rendering by default.
		String[] stackTraceAsArray = stackTraceAsString.split("[\r\n]+");

		gen.writeFieldName("stack_trace");
		gen.writeArray(stackTraceAsArray, 0, stackTraceAsArray.length);
		gen.writeEndObject();
	}

}
