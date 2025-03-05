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
package eu.solven.adhoc.filter.editor;

import eu.solven.adhoc.dag.IAdhocImplicitFilter;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.query.cube.IHasCustomMarker;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Used by {@link Shiftor}.
 * 
 * Similar to {@link IAdhocImplicitFilter} but this is expected to be used by a {@link ITransformator}.
 *
 * @author Benoit Lacelle
 */
public interface IFilterEditor {
	@Value
	@Builder
	class FilterEditorContext implements IHasCustomMarker {
		@NonNull
		IAdhocFilter filter;

		// May be null
		Object customMarker;
	}

	/**
	 *
	 * @param input
	 *            some input {@link IAdhocFilter}
	 * @return a {@link IAdhocFilter}, typically computed from the input filter.
	 */
	IAdhocFilter editFilter(IAdhocFilter input);

	/**
	 * Most {@link IFilterEditor} would not need to extend this more complex method.
	 * 
	 * @param filterEditorContext
	 * @return
	 */
	default IAdhocFilter editFilter(FilterEditorContext filterEditorContext) {
		return editFilter(filterEditorContext.getFilter());
	}
}