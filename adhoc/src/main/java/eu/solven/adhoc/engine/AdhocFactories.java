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
package eu.solven.adhoc.engine;

import eu.solven.adhoc.compression.ColumnarSliceFactory;
import eu.solven.adhoc.engine.tabular.optimizer.IFilterOptimizerFactory;
import eu.solven.adhoc.map.factory.ISliceFactoryFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.query.filter.stripper.IFilterStripperFactory;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IStopwatchFactory;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Centralize the basic factories used through Adhoc.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class AdhocFactories {
	@NonNull
	@Default
	IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	@NonNull
	@Default
	IColumnFactory columnFactory = StandardColumnFactory.builder().build();

	@NonNull
	@Default
	ISliceFactoryFactory sliceFactoryFactory = () -> ColumnarSliceFactory.builder().build();

	// @NonNull
	// @Default
	// ISliceFactory sliceFactory = ColumnarSliceFactory.builder().build();
	// DictionarizedSliceFactory.builder().build();

	@NonNull
	@Default
	IFilterOptimizerFactory filterOptimizerFactory = IFilterOptimizerFactory.standard();

	@NonNull
	@Default
	IFilterStripperFactory filterStripperFactory = AdhocUnsafe.filterStripperFactory;

	@NonNull
	@Default
	IStopwatchFactory stopwatchFactory = IStopwatchFactory.guavaStopwatchFactory();

}
