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
package eu.solven.adhoc.query.cube;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.debug.IIsDebugable;
import eu.solven.adhoc.debug.IIsExplainable;
import eu.solven.adhoc.query.IQueryOption;
import eu.solven.adhoc.query.StandardQueryOptions;

/**
 * Some Database may enable custom behavior, through additional flags. This flag would be evaluated along the DAG of
 * {@link eu.solven.adhoc.engine.step.CubeQueryStep}.
 *
 * For instance, in ActivePivot/Atoti, this could be an IContextValue.
 * 
 * @author Benoit Lacelle
 *
 */
@FunctionalInterface
public interface IHasQueryOptions extends IIsExplainable, IIsDebugable {
	Set<IQueryOption> getOptions();

	@Override
	@JsonIgnore
	default boolean isExplain() {
		return getOptions().contains(StandardQueryOptions.EXPLAIN);
	}

	@Override
	@JsonIgnore
	default boolean isDebug() {
		return getOptions().contains(StandardQueryOptions.DEBUG);
	}

	@JsonIgnore
	default boolean isDebugOrExplain() {
		return isDebug() || isExplain();
	}

	static IHasQueryOptions noOption() {
		return ImmutableSet::of;
	}
}
