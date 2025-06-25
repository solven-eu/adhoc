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
