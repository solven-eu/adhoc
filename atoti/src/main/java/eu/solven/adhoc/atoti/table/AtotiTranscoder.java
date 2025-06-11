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
package eu.solven.adhoc.atoti.table;

import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link ITableTranscoder} is useful into translating from levelNames as configured in ActivePivot processors,
 * into fieldName used in underlying {@link eu.solven.adhoc.table.ITableWrapper}. It assumes the fieldName matched the
 * levelName.
 *
 * This is useful when loading an ActivePivot configuration (e.g. transcoded from ActivePivot postprocessor properties).
 * But this is not specific to querying ActivePivot as an Adhoc Database.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class AtotiTranscoder implements ITableTranscoder {
	private static final char LEVEL_SEPARATOR = '@';

	@Override
	public String underlying(String queried) {
		int indexOfSeparator = queried.indexOf(LEVEL_SEPARATOR);
		if (indexOfSeparator >= 0) {
			// queried is typically `levelName@hierarchyname@dimensionName`. And the Database column is typically the
			// levelName.
			return queried.substring(0, indexOfSeparator);
		} else {
			return null;
		}
	}
}
