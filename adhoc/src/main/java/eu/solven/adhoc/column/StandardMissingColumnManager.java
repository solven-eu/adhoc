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
package eu.solven.adhoc.column;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.map.ICoordinateNormalizer;
import eu.solven.adhoc.map.StandardCoordinateNormalizer;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;

/**
 * A simple implementation for {@link IMissingColumnManager}. You can use it as starting-point for your projects needs.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class StandardMissingColumnManager implements IMissingColumnManager {

	@NonNull
	@Default
	ICoordinateNormalizer coordinateNormalizer = new StandardCoordinateNormalizer();

	@Override
	public Object onMissingColumn(ICubeWrapper cube, String column) {
		return "%s".formatted(cube.getName());
	}

	@Override
	public Object onMissingColumn(String column) {
		return NullMatcher.NULL_HOLDER;
	}

}
