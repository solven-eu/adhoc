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
package eu.solven.adhoc.pivotable.webflux.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * Holds various details/metadata/enrichment about the application.
 * 
 * @author Benoit Lacelle
 */
@RestController
public class PivotableMetadataController {

	private static final String K_DESC = "description";
	private static final String K_NAME = "name";

	@GetMapping(IPivotableApiConstants.PREFIX + "/public/metadata")
	@Bean
	public Map<String, ?> getMetadata() {
		// Linked as we want DEBUG and EXPLAIN first
		Map<String, Object> metadata = new LinkedHashMap<>();

		{
			List<Map<String, Object>> queryOptions = new ArrayList<>();

			queryOptions.add(ImmutableMap.<String, Object>builder()
					.put(K_NAME, StandardQueryOptions.DEBUG.toString())
					.put(K_DESC,
							"Forces the query to be executed with `debug=true`. Very fine-grained information are added in logs")
					.build());

			queryOptions.add(ImmutableMap.<String, Object>builder()
					.put(K_NAME, StandardQueryOptions.EXPLAIN.toString())
					.put(K_DESC,
							"Forces the query to be executed with `explain=true`. Query informative information are added in logs")
					.build());

			queryOptions.add(ImmutableMap.<String, Object>builder()
					.put(K_NAME, StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY.toString())
					.put(K_DESC, "Unknown measures should be treated as empty. By default, they lead to an exception.")
					.build());

			queryOptions.add(ImmutableMap.<String, Object>builder()
					.put(K_NAME, StandardQueryOptions.CONCURRENT.toString())
					.put(K_DESC, "Enable concurrency in the query execution. May expect better performance.")
					.build());

			queryOptions.add(ImmutableMap.<String, Object>builder()
					.put(K_NAME, StandardQueryOptions.NO_CACHE.toString())
					.put(K_DESC,
							"Force disabling any caching (if any are configured). Toggle-off does not add any cache by itself.")
					.build());

			for (StandardQueryOptions option : StandardQueryOptions.values()) {
				if (queryOptions.stream()
						.noneMatch(m -> MapPathGet.getRequiredString(m, K_NAME).equalsIgnoreCase(option.toString()))) {
					queryOptions.add(ImmutableMap.<String, Object>builder()
							.put(K_NAME, option.toString())
							.put(K_DESC, "Missing description for " + option.toString())
							.build());
				}
			}

			metadata.put("query_options", queryOptions);
		}

		return metadata;
	}
}