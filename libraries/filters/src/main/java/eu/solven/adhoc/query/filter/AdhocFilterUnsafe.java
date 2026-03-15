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
package eu.solven.adhoc.query.filter;

import eu.solven.adhoc.query.filter.optimizer.FilterOptimizerIntraCache;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.query.filter.stripper.FilterStripperFactory;
import eu.solven.adhoc.query.filter.stripper.IFilterStripperFactory;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Some various unsafe constants, one should edit if he knows what he's doing.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
@SuppressWarnings({ "checkstyle:MagicNumber", "PMD.MutableStaticState", "PMD.FieldDeclarationsShouldBeAtStartOfClass" })
public class AdhocFilterUnsafe {

	public static void resetAll() {
		filterOptimizer = DEFAULT_FILTER_OPTIMIZER;
		filterStripperFactory = DEFAULT_FILTER_STRIPPER_FACTORY;
	}

	/**
	 * Default {@link IFilterOptimizer}, used by static methods. As this one is maintained in the long-run, it should
	 * have no persistent cache, or with a proper expiring policy.
	 */
	private static final IFilterOptimizer DEFAULT_FILTER_OPTIMIZER = FilterOptimizerIntraCache.builder().build();
	public static IFilterOptimizer filterOptimizer = DEFAULT_FILTER_OPTIMIZER;

	/**
	 * Default {@link IFilterStripperFactory}, used by static methods. As this one is maintained in the long-run, it
	 * should have no persistent cache, or with a proper expiring policy.
	 */
	private static final IFilterStripperFactory DEFAULT_FILTER_STRIPPER_FACTORY =
			FilterStripperFactory.builder().build();
	public static IFilterStripperFactory filterStripperFactory = DEFAULT_FILTER_STRIPPER_FACTORY;

}
