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
package eu.solven.adhoc.query.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Enable filter in a CubeQuery
 * 
 * @author Benoit Lacelle
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "type",
		defaultImpl = AndFilter.class)
@JsonSubTypes({ @JsonSubTypes.Type(value = AndFilter.class, name = "and"),
		@JsonSubTypes.Type(value = OrFilter.class, name = "or"),
		@JsonSubTypes.Type(value = NotFilter.class, name = "not"),
		@JsonSubTypes.Type(value = ColumnFilter.class, name = "column"), })
// Custom serialization is configured with `AdhocPublicJackson.makeAdhocModule()`
// @JsonSerialize(using = AdhocFilterSerializer.class)
// @JsonDeserialize(using = AdhocFilterDeserializer.class)
public interface IAdhocFilter {
	IAdhocFilter MATCH_ALL = AndFilter.builder().build();
	IAdhocFilter MATCH_NONE = OrFilter.builder().build();

	/**
	 * If true, this {@link IAdhocFilter} defines points to exclude.
	 * <p>
	 * If false, this {@link IAdhocFilter} defines points to include.
	 *
	 * @return true if this is a {@link NotFilter}
	 */
	@JsonIgnore
	default boolean isNot() {
		return false;
	}

	/**
	 *
	 * @return true if this matcher matches everything
	 */
	@JsonIgnore
	default boolean isMatchAll() {
		return false;
	}

	/**
	 *
	 * @return true if this matcher does not match anything
	 */
	@JsonIgnore
	default boolean isMatchNone() {
		return false;
	}

	/**
	 * These are the most simple and primitive filters.
	 *
	 * @return true if this filters a column for a given value
	 */
	@JsonIgnore
	default boolean isColumnFilter() {
		return false;
	}

	/**
	 * @return if this is an {@link OrFilter}
	 */
	@JsonIgnore
	default boolean isOr() {
		return false;
	}

	/**
	 * @return true if this an {@link AndFilter}
	 */
	@JsonIgnore
	default boolean isAnd() {
		return false;
	}
}