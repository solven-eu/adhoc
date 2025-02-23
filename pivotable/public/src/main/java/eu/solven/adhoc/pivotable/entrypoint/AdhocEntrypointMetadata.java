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
package eu.solven.adhoc.pivotable.entrypoint;

import java.util.UUID;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Wraps an URL holding a standard Adhoc API.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
// Entrypoint is better wording than server, as we just need an URL: some API may be served by FaaS anything else than a
// plain server
public class AdhocEntrypointMetadata implements IServerMetadataConstants {
	public static final UUID SELF_ENTRYPOINT_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

	UUID id;
	String name;
	String url;

	/**
	 * 
	 * @return an entrypoint provided by Pivotable instance itself.
	 */
	public static AdhocEntrypointMetadata localhost() {
		return AdhocEntrypointMetadata.builder()
				.id(SELF_ENTRYPOINT_ID)
				.name("localhost:self")
				.url("http://localhost:self")
				.build();
	}
}
