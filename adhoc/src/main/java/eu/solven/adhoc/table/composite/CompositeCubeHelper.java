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
package eu.solven.adhoc.table.composite;

import java.util.Set;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.UtilityClass;

/**
 * Helpers dedicated to {@link CompositeCubesTableWrapper}.
 * 
 * @author Benoit Lacelle
 */
@SuppressWarnings("PMD.MissingStaticMethodInNonInstantiatableClass")
@UtilityClass
public class CompositeCubeHelper {
	/**
	 * Details {@link IMeasure} which are compatible given an {@link ICubeQuery} and a {@link ICubeWrapper}.
	 * 
	 * @author Benoit Lacelle
	 */
	@Value
	@Builder
	public static class CompatibleMeasures {

		// Measures which are known by the subCube, which we refer to
		Set<IMeasure> predefined;

		// Measures which are not known by the subCube, but for which we provide a definition
		Set<Aggregator> defined;

		public boolean isEmpty() {
			return predefined.isEmpty() && defined.isEmpty();
		}
	}
}
