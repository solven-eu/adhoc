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

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.pivottable.api.IPivotableApiConstants;

/**
 * Holds various details/metadata/enrichment about the application.
 */
@RestController
public class PivotableMetadataController {

	@GetMapping(IPivotableApiConstants.PREFIX + "/public/metadata")
	@Bean
	public Map<String, ?> getMetadata() {
		Map<String, Object> metadata = new LinkedHashMap<>();

		// {
		// Map<String, Object> tagsMetadata = new LinkedHashMap<>();
		// tagsMetadata.put(IGameMetadataConstants.TAG_OPTIMIZATION,
		// "The goal is to find the best solution for given problem. Any player can join.");
		// tagsMetadata.put(IGameMetadataConstants.TAG_1V1,
		// "We need exactly 2 players, to play one against the other");
		// tagsMetadata.put(IGameMetadataConstants.TAG_PERFECT_INFORMATION, "All players can see the whole board");
		//
		// metadata.put("tags", tagsMetadata);
		// }

		return metadata;
	}
}