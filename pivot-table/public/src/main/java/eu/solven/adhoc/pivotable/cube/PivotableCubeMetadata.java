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
package eu.solven.adhoc.pivotable.cube;

import java.util.Set;
import java.util.UUID;

import eu.solven.adhoc.pivotable.entrypoint.AdhocEntrypointMetadata;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Each {@link AdhocEntrypointMetadata} has a {@link Set} of available cubes. This is one of such cube.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class PivotableCubeMetadata {

	@NonNull
	UUID entrypointId;

	@NonNull
	UUID id;

	@NonNull
	String name;

	@NonNull
	Set<String> measures;

	@NonNull
	Set<String> columns;

	public static PivotableCubeMetadataBuilder fromEntrypoint(AdhocEntrypointMetadata game) {
		return PivotableCubeMetadata.builder().entrypointId(game.getId());
	}

	/**
	 * 
	 * @return an invalid {@link PivotableCubeMetadata}. Useful in edge-cases, like processing race-conditions and
	 *         removed contests.
	 */
	// public static PivotableCubeMetadata empty() {
	// return PivotableCubeMetadata.builder()
	// .gameId(IServerMetadataConstants.EMPTY)
	// .name("empty")
	// .author(UUID.fromString("12345678-1234-1234-1234-123456789012"))
	// .minPlayers(Integer.MIN_VALUE)
	// .maxPlayers(Integer.MIN_VALUE)
	// .build();
	// }

}
