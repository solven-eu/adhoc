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
package eu.solven.adhoc.column;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.coordinate.ICalculatedCoordinate;
import eu.solven.adhoc.coordinate.IHasCalculatedCoordinates;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * An {@link IAdhocColumn} which is enriched with additional {@link ICalculatedCoordinate}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class ColumnWithCalculatedCoordinates implements IAdhocColumn, IHasCalculatedCoordinates {

	@NonNull
	IAdhocColumn column;

	@Singular
	ImmutableList<ICalculatedCoordinate> calculatedCoordinates;

	@Override
	// JsonIgnore as we rely on the column which may not be a ReferencedColumn
	@JsonIgnore
	public String getName() {
		return column.getName();
	}

	/**
	 * Lombok Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class ColumnWithCalculatedCoordinatesBuilder {
		@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
		IAdhocColumn column;

		public ColumnWithCalculatedCoordinatesBuilder column(IAdhocColumn column) {
			this.column = column;
			return this;
		}

		public ColumnWithCalculatedCoordinatesBuilder column(String column) {
			return column(ReferencedColumn.ref(column));
		}
	}

}
