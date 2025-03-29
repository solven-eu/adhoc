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
package eu.solven.adhoc.measure;

import eu.solven.adhoc.dag.AdhocQueryEngine;

/**
 * For classes holding an {@link IOperatorsFactory}
 * 
 * @author Benoit Lacelle
 */
public interface IHasOperatorsFactory {
	IOperatorsFactory getOperatorsFactory();

	/**
	 * 
	 * @param o
	 *            typically a {@link AdhocQueryEngine}
	 * @return an {@link IOperatorsFactory}, a default one if the input does not implement {@link IHasOperatorsFactory}.
	 */
	static IOperatorsFactory getOperatorsFactory(Object o) {
		if (o instanceof IHasOperatorsFactory hasOperatorsFactory) {
			return hasOperatorsFactory.getOperatorsFactory();
		} else {
			return new StandardOperatorsFactory();
		}
	}
}
