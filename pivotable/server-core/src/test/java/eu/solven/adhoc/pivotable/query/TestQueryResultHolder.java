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
package eu.solven.adhoc.pivotable.query;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import eu.solven.adhoc.data.tabular.IReadableTabularView;
import eu.solven.adhoc.data.tabular.ListBasedTabularView;
import eu.solven.pepper.unittest.PepperJacksonTestHelper;

public class TestQueryResultHolder {
	@Test
	public void testRetryIn_Jackson() {
		QueryResultHolder holder = QueryResultHolder.retry(AsynchronousStatus.RUNNING, Duration.ofSeconds(5));

		ObjectMapper objectMapper = PepperJacksonTestHelper.makeObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);

		String asString = PepperJacksonTestHelper.asString(objectMapper, QueryResultHolder.class, holder);

		Assertions.assertThat(asString).isEqualTo("""
				{
				  "state" : "RUNNING",
				  "retryIn" : "PT5S",
				  "retryInMs" : 5000
				}""");
	}

	@Test
	public void testView_Jackson() {
		IReadableTabularView view = ListBasedTabularView.builder()
				.coordinates(List.of(Map.of("c", "c1")))
				.values(List.of(Map.of("m", "v")))
				.build();
		QueryResultHolder holder = QueryResultHolder.served(AsynchronousStatus.SERVED, view);

		ObjectMapper objectMapper = PepperJacksonTestHelper.makeObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.enable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);

		String asString = PepperJacksonTestHelper.asString(objectMapper, QueryResultHolder.class, holder);

		Assertions.assertThat(asString).isEqualTo("""
				{
				  "state" : "SERVED",
				  "view" : {
				    "coordinates" : [ {
				      "c" : "c1"
				    } ],
				    "values" : [ {
				      "m" : "v"
				    } ]
				  }
				}""");
	}
}
