/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure;

import java.util.Set;

import eu.solven.adhoc.measure.model.IMeasure;

/**
 * Used to interact with a measure forest. Can be used for both read and write operations.
 * 
 * @author Benoit Lacelle
 */
public interface IMeasureForestVisitor {

	default Set<IMeasure> addMeasures() {
		return Set.of();
	}

	/**
	 * 
	 * @param measure
	 * @return the Set of {@link IMeasure} which has to replace the input {@link IMeasure}. Can be a {@link Set} of 0, 1
	 *         or more IMeasure.
	 */
	default Set<IMeasure> mapMeasure(IMeasure measure) {
		return Set.of(measure);
	}

}
