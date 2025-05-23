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
package eu.solven.adhoc.table.transcoder.value;

import java.time.LocalDate;
import java.util.Set;

/**
 * Enable transcoding from a raw column coordinate to a proper column coordinate.
 * 
 * This is typically used to transcode from {@link String} to {@link LocalDate}.
 * 
 * @author Benoit Lacelle
 */
public interface IColumnValueTranscoder {

	/**
	 * A sub-Set of columns which are eligible for transcoding.
	 * 
	 * @param columns
	 *            the columns of current records
	 * @return candidate for transcoding. Should be a sub-set of input columns
	 */
	Set<String> mayTranscode(Set<String> columns);

	/**
	 * 
	 * @param column
	 * @param value
	 * @return the transcoded value.
	 */
	Object transcodeValue(String column, Object value);
}
