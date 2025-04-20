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
package eu.solven.adhoc.beta.schema;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.Value;

/**
 * Helps defining a customMarker single data-point. Typically used for UI read/write of customMarkers.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class CustomMarkerMetadataGenerator {
	// jsonPath to the data-point
	String path;

	// String type;

	Supplier<Set<String>> possibleValues;

	// Helps indicating the default value when none is provided
	Supplier<Optional<String>> defaultValue;

	public CustomMarkerMetadata snapshot(String name) {
		return CustomMarkerMetadata.builder()
				.name(name)
				.path(path)
				.possibleValues(possibleValues.get())
				.defaultValue(defaultValue.get().orElse(null))
				.build();
	}
}
