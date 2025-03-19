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
package eu.solven.adhoc.column;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import eu.solven.adhoc.query.filter.value.IHasWrapped;
import eu.solven.adhoc.resource.HasWrappedSerializer;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * A simple column, explicitly referred.
 * 
 * @author Benoit Lacelle
 *
 */
@Builder
@Value
@Jacksonized
@JsonSerialize(using = HasWrappedSerializer.class)
public class ReferencedColumn implements IAdhocColumn, IHasWrapped {
	// https://github.com/FasterXML/jackson-databind/issues/5030
	// @JsonValue
	String name;

	@Override
	public Object getWrapped() {
		return getName();
	}

	public static ReferencedColumn ref(String column) {
		return ReferencedColumn.builder().name(column).build();
	}

	public static class ReferencedColumnBuilder {

		public ReferencedColumnBuilder() {
		}

		// Enable Jackson deserialization given a plain String
		public ReferencedColumnBuilder(String column) {

			this.name(column);
		}
	}
}
