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
package eu.solven.adhoc.pivotable.webmvc.api;

import java.util.List;
import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.adhoc.pivotable.api.IPivotableApiConstants;
import eu.solven.adhoc.pivotable.cube.AdhocCubesRegistry;
import eu.solven.adhoc.pivotable.cube.PivotableCubeMetadata;
import eu.solven.adhoc.pivotable.cube.PivotableCubeSearchParameters;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Lists available cubes.
 *
 * @author Benoit Lacelle
 * @deprecated Functionality is superseded by the endpoints/schemas API.
 */
@Deprecated(since = "used?")
@RequiredArgsConstructor
@Slf4j
@RestController
@RequestMapping(IPivotableApiConstants.PREFIX)
public class CubesController {

	final AdhocCubesRegistry cubesRegistry;

	/**
	 * @param endpointId
	 *            optional filter by endpoint UUID
	 * @param cube
	 *            optional filter by cube name
	 * @param keyword
	 *            optional keyword search
	 * @return list of matching {@link PivotableCubeMetadata}
	 */
	@GetMapping(value = "/cubes", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<PivotableCubeMetadata> listCubes(@RequestParam(required = false) String endpointId,
			@RequestParam(required = false) String cube,
			@RequestParam(required = false) String keyword) {
		PivotableCubeSearchParameters.PivotableCubeSearchParametersBuilder parameters =
				PivotableCubeSearchParameters.builder();

		if (endpointId != null) {
			parameters.endpointId(Optional.of(java.util.UUID.fromString(endpointId)));
		}
		if (cube != null) {
			parameters.cube(Optional.of(cube));
		}
		if (keyword != null) {
			parameters.keyword(Optional.of(keyword));
		}

		List<PivotableCubeMetadata> cubes = cubesRegistry.searchCubes(parameters.build());
		log.debug("Cubes for {}: {}", parameters, cubes);
		return cubes;
	}
}
