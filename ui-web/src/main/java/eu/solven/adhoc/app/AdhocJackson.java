package eu.solven.adhoc.app;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AdhocJackson {
	public static ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();

		objectMapper.registerModule(new JavaTimeModule());

		// https://stackoverflow.com/questions/76225352/spring-boot-jackson-date-format
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

		return objectMapper;
	}

	public static <T> T clone(T t) {
		ObjectMapper objectMapper = objectMapper();

		T asObject;
		try {
			byte[] asString = objectMapper.writeValueAsBytes(t);
			asObject = (T) objectMapper.readValue(asString, t.getClass());
		} catch (IOException e) {
			throw new UncheckedIOException("Not clonable: " + t, e);
		}

		return asObject;
	}
}