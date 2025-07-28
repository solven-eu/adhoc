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

import java.util.function.Function;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * A {@link FunctionCalculatedColumn} is a column which is evaluated programmatically by Adhoc. As it is based on a
 * {@link Function}, it can not be serialized into JSON.
 * 
 * It is evaluated on a per-row basis, given the rows provided by the {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class FunctionCalculatedColumn implements IAdhocColumn, ICalculatedColumn {
	// The name of the evaluated column
	@NonNull
	String name;

	@NonNull
	@Default
	Class<?> type = Object.class;

	// Compute a coordinate given current record
	Function<ITabularRecord, Object> recordToCoordinate;

	// TODO Bad-design. Used for `*` calculated column, as given coordinate should not be filtered-out by
	// IValueMatchers.
	@Default
	boolean skipFiltering = false;

	@Override
	public Object computeCoordinate(ITabularRecord record) {
		return recordToCoordinate.apply(record);
	}

	@Override
	public Class<?> getType() {
		return type;
	}

}
