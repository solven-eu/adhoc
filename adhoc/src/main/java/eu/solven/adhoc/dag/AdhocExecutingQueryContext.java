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
package eu.solven.adhoc.dag;

import java.util.Set;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.transformers.EmptyMeasure;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Holds the living action doing a query, typically while being executed by an {@link AdhocQueryEngine}
 * 
 * @author Benoit Lacelle
 */
@Builder
@Value
public class AdhocExecutingQueryContext implements IIsExplainable, IIsDebugable {
	// The query requested to the queryEngine
	@NonNull
	IAdhocQuery query;

	// an IAdhocQuery is executed relatively to a measureBag as requested measure depends (implicitly) on underlying
	// measures
	@NonNull
	final IAdhocMeasureBag measures;

	@NonNull
	@Singular
	Set<? extends IQueryOption> options;

	protected IMeasure resolveIfRef(IMeasure measure) {
		if (measure == null) {
			throw new IllegalArgumentException("Null input");
		}

		if (options.contains(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY)) {
			if (measure instanceof ReferencedMeasure ref) {
				return this.measures.resolveIfRefOpt(ref).orElseGet(() -> new EmptyMeasure(ref.getRef()));
			} else {
				return this.measures.resolveIfRefOpt(measure).orElseGet(() -> new EmptyMeasure(measure.getName()));
			}
		} else {
			return this.measures.resolveIfRef(measure);
		}
	}

	public boolean isExplain() {
		// TODO queryOptions may force explain
		return getQuery().isExplain();
	}

	public boolean isDebug() {
		// TODO queryOptions may force explain
		return getQuery().isDebug();
	}
}
