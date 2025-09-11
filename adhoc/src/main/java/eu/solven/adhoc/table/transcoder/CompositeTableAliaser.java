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
package eu.solven.adhoc.table.transcoder;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;

/**
 * Helps combining multiple {@link ITableAliaser}.
 * 
 * @author Benoit Lacelle
 */
@Builder
public class CompositeTableAliaser implements ITableAliaser, IHasAliasedColumns {
	/**
	 * Different modes when iterating through the available transcoders.
	 * 
	 * @author Benoit Lacelle
	 */
	@SuppressWarnings("PMD.FieldNamingConventions")
	public enum ChainMode {
		/**
		 * Should we return the first not-null underlying
		 */
		FirstNotNull,

		/**
		 * All transcoders, as a chain of transcoders
		 */
		ApplyAll
	}

	@NonNull
	@Singular
	ImmutableList<ITableAliaser> aliasers;

	@Default
	@NonNull
	ChainMode chainMode = ChainMode.FirstNotNull;

	@Override
	public String underlying(String queried) {
		boolean oneMatched = false;
		String currenQueried = queried;

		for (ITableAliaser aliaser : aliasers) {
			String underlying = aliaser.underlying(currenQueried);

			if (underlying != null) {
				oneMatched = true;

				if (chainMode == ChainMode.FirstNotNull) {
					return underlying;
				} else {
					currenQueried = underlying;
				}
			}
		}

		// null means `not transcoded`
		if (oneMatched) {
			return currenQueried;
		} else {
			return null;
		}
	}

	@Override
	public Set<String> getAlias() {
		return aliasers.stream().flatMap(a -> {
			if (a instanceof IHasAliasedColumns hasAliases) {
				return hasAliases.getAlias().stream();
			} else {
				return Stream.of();
			}
		}).collect(Collectors.toSet());
	}

}
